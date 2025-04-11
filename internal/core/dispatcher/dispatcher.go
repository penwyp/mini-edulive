// internal/core/dispatcher/dispatcher.go
package dispatcher

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability" // 引入可观测性包
	"github.com/penwyp/mini-edulive/internal/core/websocket"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/quic-go/quic-go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute" // 用于添加 Span 属性
	"go.opentelemetry.io/otel/trace"     // 用于 Span 操作
	"go.uber.org/zap"
)

// BulletMessage 切片的池，初始容量为 100
var (
	bulletSlicePool = pool.RegisterPool("bullet_slice", func() []*protocol.BulletMessage {
		return make([]*protocol.BulletMessage, 0, 100)
	})
	serializedBulletPool = pool.RegisterPool("serialized_bullet", func() *protocol.SerializedBullet {
		return &protocol.SerializedBullet{}
	})
)

type Dispatcher struct {
	config       *config.Config
	redisClient  *redis.ClusterClient
	keyBuilder   *pkgcache.RedisKeyBuilder
	quicListener *quic.Listener
	clients      map[uint64]*ClientInfo // 存储 userID 到 ClientInfo 的映射
	mutex        sync.RWMutex
	wg           sync.WaitGroup
}

type ClientInfo struct {
	Stream   quic.Stream
	LiveID   uint64
	UserName string
}

func NewDispatcher(spanCtx context.Context, cfg *config.Config) (*Dispatcher, error) {
	// 创建根 Span 追踪 Dispatcher 初始化
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.NewDispatcher")
	defer span.End()

	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		observability.RecordError(span, err, "dispatcher.NewDispatcher")
		return nil, err
	}

	startTime := time.Now()
	quicListener, err := quic.ListenAddr(cfg.Distributor.QUIC.Addr, util.GenerateTLSConfig(cfg.Distributor.QUIC.CertFile, cfg.Distributor.QUIC.KeyFile), nil)
	if err != nil {
		logger.Error("Failed to start QUIC listener", zap.Error(err))
		observability.RecordError(span, err, "dispatcher.NewDispatcher")
		return nil, err
	}
	observability.RecordLatency(ctx, "quic.ListenAddr", time.Since(startTime))

	d := &Dispatcher{
		config:       cfg,
		redisClient:  redisClient,
		keyBuilder:   pkgcache.NewRedisKeyBuilder(),
		quicListener: quicListener,
		clients:      make(map[uint64]*ClientInfo),
	}
	return d, nil
}

func (d *Dispatcher) Start(spanCtx context.Context) {
	// 创建根 Span 追踪 Dispatcher 启动
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.Start",
		trace.WithAttributes(
			attribute.String("quic_addr", d.config.Distributor.QUIC.Addr),
		))
	defer span.End()

	logger.Info("Dispatcher started", zap.String("quic_addr", d.config.Distributor.QUIC.Addr))

	d.wg.Add(1)
	go d.acceptConnections(ctx)

	d.wg.Add(1)
	go d.pushBulletLoop(ctx)

	d.wg.Wait()
}

func (d *Dispatcher) acceptConnections(spanCtx context.Context) {
	defer d.wg.Done()
	for {
		// 创建 Span 追踪 QUIC 连接接受
		ctx, span := observability.StartSpan(spanCtx, "dispatcher.acceptConnections")
		startTime := time.Now()

		conn, err := d.quicListener.Accept(ctx)
		if err != nil {
			logger.Error("Failed to accept QUIC connection", zap.Error(err))
			observability.RecordError(span, err, "dispatcher.acceptConnections")
			span.End()
			time.Sleep(time.Second)
			continue
		}
		observability.RecordLatency(ctx, "quic.Accept", time.Since(startTime))
		span.End()

		go d.handleConnection(ctx, conn)
	}
}

func (d *Dispatcher) handleConnection(spanCtx context.Context, conn quic.Connection) {
	// 创建 Span 追踪单个连接处理
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.handleConnection")
	defer span.End()

	defer conn.CloseWithError(0, "connection closed")

	startTime := time.Now()
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		logger.Error("Failed to accept QUIC stream", zap.Error(err))
		observability.RecordError(span, err, "dispatcher.handleConnection")
		return
	}
	observability.RecordLatency(ctx, "quic.AcceptStream", time.Since(startTime))

	// 读取初始化消息
	data, err := readStream(ctx, stream)
	if err != nil {
		logger.Error("Failed to read initial message", zap.Error(err))
		observability.RecordError(span, err, "dispatcher.handleConnection")
		stream.Close()
		return
	}

	msg, err := protocol.Decode(data)
	if err != nil {
		logger.Error("Failed to decode initial message", zap.Error(err))
		observability.RecordError(span, err, "dispatcher.handleConnection")
		stream.Close()
		return
	}
	defer msg.Release()

	span.SetAttributes(
		attribute.Int64("user_id", int64(msg.UserID)),
		attribute.Int64("live_id", int64(msg.LiveID)),
		attribute.String("user_name", msg.UserName),
	)

	d.mutex.Lock()
	d.clients[msg.UserID] = &ClientInfo{
		Stream:   stream,
		LiveID:   msg.LiveID,
		UserName: msg.UserName,
	}
	d.mutex.Unlock()

	logger.Info("New client connected",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))

	// 保持流活跃
	for {
		_, err := readStream(ctx, stream)
		if err != nil {
			logger.Warn("Stream read error, removing client",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName),
				zap.Error(err))
			observability.RecordError(span, err, "dispatcher.handleConnection")
			d.removeClient(ctx, msg.UserID)
			return
		}
	}
}

func (d *Dispatcher) pushBulletLoop(spanCtx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		// 创建 Span 追踪单次弹幕推送循环
		ctx, span := observability.StartSpan(spanCtx, "dispatcher.pushBulletLoop")
		startTime := time.Now()

		d.pushBullets(ctx)
		observability.RecordLatency(ctx, "dispatcher.pushBullets", time.Since(startTime))
		span.End()
	}
}

func (d *Dispatcher) pushBullets(spanCtx context.Context) {
	// 创建 Span 追踪弹幕推送
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.pushBullets")
	defer span.End()

	startTime := time.Now()
	bullets, err := d.fetchTopBullets(ctx)
	if err != nil {
		logger.Error("Failed to fetch top bullets", zap.Error(err))
		observability.RecordError(span, err, "dispatcher.pushBullets")
		return
	}

	if len(bullets) == 0 {
		bulletSlicePool.Put(bullets)
		return
	}
	span.SetAttributes(attribute.Int("bullet_count", len(bullets)))

	// 按 liveID 分组弹幕
	bulletsByLiveID := make(map[uint64][]*protocol.BulletMessage)
	for _, bullet := range bullets {
		bulletsByLiveID[bullet.LiveID] = append(bulletsByLiveID[bullet.LiveID], bullet)
	}

	d.mutex.RLock()
	for userID, clientInfo := range d.clients {
		clientBullets, ok := bulletsByLiveID[clientInfo.LiveID]
		if !ok || len(clientBullets) == 0 {
			continue
		}

		// 创建子 Span 追踪单客户端推送
		clientCtx, clientSpan := observability.StartSpan(ctx, "dispatcher.pushToClient",
			trace.WithAttributes(
				attribute.Int64("user_id", int64(userID)),
				attribute.Int64("live_id", int64(clientInfo.LiveID)),
			))
		startTime = time.Now()

		data, err := d.encodeBullets(ctx, clientBullets)
		if err != nil {
			logger.Error("Failed to encode bullets for client",
				zap.Uint64("userID", userID),
				zap.String("userName", clientInfo.UserName),
				zap.Error(err))
			observability.RecordError(clientSpan, err, "dispatcher.pushToClient")
			clientSpan.End()
			continue
		}

		err = d.sendToClient(ctx, clientInfo.Stream, data)
		if err != nil {
			logger.Warn("Failed to send to client",
				zap.Uint64("userID", userID),
				zap.String("userName", clientInfo.UserName),
				zap.Error(err))
			observability.RecordError(clientSpan, err, "dispatcher.pushToClient")
			d.removeClient(ctx, userID)
		}
		observability.RecordLatency(clientCtx, "dispatcher.pushToClient", time.Since(startTime))
		clientSpan.End()
	}
	d.mutex.RUnlock()

	// 归还资源
	for _, bullet := range bullets {
		bullet.Release()
	}
	bulletSlicePool.Put(bullets)

	logger.Debug("Bullets pushed",
		zap.Int("bullet_count", len(bullets)),
		zap.Duration("total_time", time.Since(startTime)))
	observability.RecordLatency(ctx, "dispatcher.pushBullets", time.Since(startTime))
}

func (d *Dispatcher) encodeBullets(spanCtx context.Context, bullets []*protocol.BulletMessage) ([]byte, error) {
	// 创建 Span 追踪弹幕编码
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.encodeBullets",
		trace.WithAttributes(
			attribute.Int("bullet_count", len(bullets)),
		))
	defer span.End()

	startTime := time.Now()
	bufPool, err := pool.GetPool[*bytes.Buffer]("bytes_buffer")
	if err != nil {
		observability.RecordError(span, err, "dispatcher.encodeBullets")
		return nil, fmt.Errorf("failed to get bytes_buffer pool: %v", err)
	}
	buf := bufPool.Get()
	buf.Reset()
	defer bufPool.Put(buf)

	for _, bullet := range bullets {
		data, err := bullet.Encode(d.config.Performance.BulletCompression)
		if err != nil {
			logger.Warn("Failed to encode bullet", zap.Error(err))
			observability.RecordError(span, err, "dispatcher.encodeBullets")
			continue
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
			observability.RecordError(span, err, "dispatcher.encodeBullets")
			return nil, err
		}
		buf.Write(data)
	}
	logger.Debug("Encoded bullets", zap.Int("bytes", buf.Len()))
	observability.RecordLatency(ctx, "dispatcher.encodeBullets", time.Since(startTime))
	return buf.Bytes(), nil
}

func (d *Dispatcher) sendToClient(spanCtx context.Context, stream quic.Stream, data []byte) error {
	// 创建 Span 追踪 QUIC 发送
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.sendToClient")
	defer span.End()

	startTime := time.Now()
	n, err := stream.Write(data)
	logger.Debug("Sent to client", zap.Int("bytes", n), zap.Error(err))
	if err != nil {
		observability.RecordError(span, err, "dispatcher.sendToClient")
	}
	observability.RecordLatency(ctx, "quic.Write", time.Since(startTime))
	return err
}

func (d *Dispatcher) fetchTopBullets(spanCtx context.Context) ([]*protocol.BulletMessage, error) {
	// 创建 Span 追踪弹幕获取
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.fetchTopBullets")
	defer span.End()

	allBullets := bulletSlicePool.Get()
	allBullets = allBullets[:0]

	startTime := time.Now()
	activeRooms, err := d.redisClient.SMembers(ctx, d.keyBuilder.ActiveLiveRoomsKey()).Result()
	if err != nil {
		bulletSlicePool.Put(allBullets)
		observability.RecordError(span, err, "dispatcher.fetchTopBullets")
		return nil, err
	}
	observability.RecordLatency(ctx, "redis.SMembers", time.Since(startTime))
	if len(activeRooms) == 0 {
		activeRooms = []string{util.FormatUint64ToString(websocket.BackDoorLiveRoomID)}
	}

	for _, roomStr := range activeRooms {
		liveID, err := strconv.ParseUint(roomStr, 10, 64)
		if err != nil {
			logger.Warn("Invalid liveID in active rooms", zap.String("liveID", roomStr), zap.Error(err))
			observability.RecordError(span, err, "dispatcher.fetchTopBullets")
			continue
		}
		bulletKey := d.keyBuilder.LiveBulletKey(liveID)

		startTime = time.Now()
		serializedContent, err := d.redisClient.LPop(ctx, bulletKey).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			logger.Warn("Failed to fetch bullet for liveID", zap.Uint64("liveID", liveID), zap.Error(err))
			observability.RecordError(span, err, "dispatcher.fetchTopBullets")
			continue
		}
		observability.RecordLatency(ctx, "redis.LPop", time.Since(startTime))

		serializedBullet := serializedBulletPool.Get()
		serializedBullet.Reset()
		defer serializedBulletPool.Put(serializedBullet)

		// 反序列化 JSON 数据
		if err := json.Unmarshal([]byte(serializedContent), &serializedBullet); err != nil {
			logger.Warn("Failed to unmarshal serialized bullet",
				zap.String("serialized_content", serializedContent),
				zap.Error(err))
			observability.RecordError(span, err, "dispatcher.fetchTopBullets")
			continue
		}

		bullet := protocol.NewBulletMessage(
			serializedBullet.LiveID,
			serializedBullet.UserID,
			serializedBullet.UserName,
			serializedBullet.Content,
			serializedBullet.Color,
		)
		bullet.Timestamp = serializedBullet.Timestamp
		bullet.Color = util.GetDefaultColorOrRandom(serializedBullet.Color)

		allBullets = append(allBullets, bullet)
		logger.Debug("Fetched and parsed bullet",
			zap.Uint64("liveID", bullet.LiveID),
			zap.Uint64("userID", bullet.UserID),
			zap.String("username", bullet.UserName),
			zap.String("content", bullet.Content),
			zap.String("color", bullet.Color),
		)

		if len(allBullets) >= 10000 {
			break
		}
	}

	span.SetAttributes(attribute.Int("bullet_count", len(allBullets)))
	return allBullets, nil
}

func (d *Dispatcher) removeClient(spanCtx context.Context, userID uint64) {
	// 创建 Span 追踪客户端移除
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.removeClient",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
		))
	defer span.End()

	d.mutex.Lock()
	if clientInfo, ok := d.clients[userID]; ok {
		startTime := time.Now()
		clientInfo.Stream.Close()
		delete(d.clients, userID)
		observability.RecordLatency(ctx, "quic.Stream.Close", time.Since(startTime))
	}
	d.mutex.Unlock()
}

func (d *Dispatcher) Close(spanCtx context.Context) {
	// 创建 Span 追踪 Dispatcher 关闭
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.Close")
	defer span.End()

	startTime := time.Now()
	d.quicListener.Close()
	d.redisClient.Close()
	d.wg.Wait()
	observability.RecordLatency(ctx, "dispatcher.Close", time.Since(startTime))
	logger.Info("Dispatcher closed")
}

func readStream(spanCtx context.Context, stream quic.Stream) ([]byte, error) {
	// 创建 Span 追踪流读取
	ctx, span := observability.StartSpan(spanCtx, "dispatcher.readStream")
	defer span.End()

	startTime := time.Now()
	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		observability.RecordError(span, err, "dispatcher.readStream")
	}
	observability.RecordLatency(ctx, "quic.Read", time.Since(startTime))
	return buf[:n], err
}
