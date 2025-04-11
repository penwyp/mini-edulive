package dispatcher

import (
	"context"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/quic-go/quic-go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Dispatcher 负责管理弹幕分发的主要组件
type Dispatcher struct {
	config      *config.Config
	clientMgr   *ClientManager
	bulletMgr   *BulletManager
	roomMgr     *RoomManager
	quicServer  *QuicServer
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
	wg          sync.WaitGroup
}

// Options 配置 Dispatcher 的选项
type Options struct {
	PushInterval      time.Duration
	MaxBulletsPerRoom int
	MaxTotalBullets   int
	WorkerBufferSize  int
}

// DefaultOptions 返回默认的配置选项
func DefaultOptions() Options {
	return Options{
		PushInterval:      10 * time.Millisecond, // 10ms 拉取周期，匹配高实时性需求
		MaxBulletsPerRoom: 1000,
		MaxTotalBullets:   10000,
		WorkerBufferSize:  100,
	}
}

// NewDispatcher 创建一个新的弹幕分发器
func NewDispatcher(spanCtx context.Context, cfg *config.Config) (*Dispatcher, error) {
	_, span := ob.StartSpan(spanCtx, "dispatcher.NewDispatcher")
	defer span.End()

	// 创建 Redis 客户端
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		ob.RecordError(span, err, "dispatcher.NewDispatcher")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "setup").Inc()
		return nil, err
	}

	// 创建 Redis 键构建器
	keyBuilder := pkgcache.NewRedisKeyBuilder()

	// 创建房间管理器
	roomMgr := NewRoomManager(redisClient, keyBuilder)

	// 创建客户端管理器
	clientMgr := NewClientManager()

	// 创建弹幕管理器
	bulletMgr := NewBulletManager(redisClient, keyBuilder, cfg.Performance.BulletCompression)

	// 创建 QUIC 服务器
	quicServer, err := NewQuicServer(cfg.Distributor.QUIC.Addr, cfg.Distributor.QUIC.CertFile, cfg.Distributor.QUIC.KeyFile)
	if err != nil {
		logger.Error("Failed to create QUIC server", zap.Error(err))
		ob.RecordError(span, err, "dispatcher.NewDispatcher")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "setup").Inc()
		return nil, err
	}

	d := &Dispatcher{
		config:      cfg,
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
		roomMgr:     roomMgr,
		clientMgr:   clientMgr,
		bulletMgr:   bulletMgr,
		quicServer:  quicServer,
	}

	return d, nil
}

// Start 启动弹幕分发器
func (d *Dispatcher) Start(spanCtx context.Context) {
	ctx, span := ob.StartSpan(spanCtx, "dispatcher.Start",
		trace.WithAttributes(
			attribute.String("quic_addr", d.config.Distributor.QUIC.Addr),
		))
	defer span.End()

	logger.Info("Dispatcher started", zap.String("quic_addr", d.config.Distributor.QUIC.Addr))

	// 启动连接接收
	d.wg.Add(1)
	go d.acceptConnections(ctx)

	// 启动弹幕推送循环
	d.wg.Add(1)
	go d.runBulletPushLoop(ctx)

	d.wg.Wait()
}

// acceptConnections 接受新的 QUIC 连接
func (d *Dispatcher) acceptConnections(ctx context.Context) {
	defer d.wg.Done()

	d.quicServer.Start(ctx, func(conn quic.Connection) {
		d.handleConnection(ctx, conn)
	})
}

// handleConnection 处理单个 QUIC 连接
func (d *Dispatcher) handleConnection(ctx context.Context, conn quic.Connection) {
	ctx, span := ob.StartSpan(ctx, "dispatcher.handleConnection")
	defer span.End()

	defer func() {
		startTime := time.Now()
		err := conn.CloseWithError(0, "connection closed")
		if err != nil {
			logger.Error("Failed to close QUIC connection", zap.Error(err))
			ob.RecordError(span, err, "dispatcher.handleConnection")
			ob.QUICConnectionErrors.WithLabelValues("dispatcher", "close").Inc()
		}
		ob.ActiveQUICConnections.WithLabelValues("dispatcher").Dec()
		ob.RecordLatency(ctx, "quic.CloseConnection", time.Since(startTime))
	}()

	// 接受 QUIC 流
	stream, err := d.quicServer.AcceptStream(ctx, conn)
	if err != nil {
		logger.Error("Failed to accept QUIC stream", zap.Error(err))
		ob.RecordError(span, err, "dispatcher.handleConnection")
		return
	}

	// 读取初始消息
	msg, err := d.quicServer.ReadInitialMessage(ctx, stream)
	if err != nil {
		logger.Error("Failed to read initial message", zap.Error(err))
		ob.RecordError(span, err, "dispatcher.handleConnection")
		return
	}
	defer msg.Release()

	span.SetAttributes(
		attribute.Int64("user_id", int64(msg.UserID)),
		attribute.Int64("live_id", int64(msg.LiveID)),
		attribute.String("user_name", msg.UserName),
	)

	// 创建客户端信息并注册
	client := &Client{
		UserID:   msg.UserID,
		LiveID:   msg.LiveID,
		UserName: msg.UserName,
		Stream:   stream,
	}
	d.clientMgr.AddClient(client)

	logger.Info("New client connected",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))

	// 持续读取消息，作为心跳检测
	for {
		_, err := d.quicServer.ReadMessage(ctx, stream)
		if err != nil {
			logger.Warn("Stream read error, removing client",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName),
				zap.Error(err))
			ob.RecordError(span, err, "dispatcher.handleConnection")
			ob.QUICConnectionErrors.WithLabelValues("dispatcher", "read").Inc()
			d.clientMgr.RemoveClient(ctx, msg.UserID)
			return
		}
	}
}

// runBulletPushLoop 运行弹幕推送循环
func (d *Dispatcher) runBulletPushLoop(ctx context.Context) {
	defer d.wg.Done()

	// 创建一个定时器，定期推送弹幕
	opts := DefaultOptions()
	ticker := time.NewTicker(opts.PushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context canceled, stopping bullet push loop", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			pushCtx, span := ob.StartSpan(ctx, "dispatcher.bulletPushCycle")
			startTime := time.Now()

			d.pushBulletsCycle(pushCtx)

			ob.RecordLatency(pushCtx, "dispatcher.bulletPushCycle", time.Since(startTime))
			span.End()
		}
	}
}

// pushBulletsCycle 执行单次弹幕推送周期
func (d *Dispatcher) pushBulletsCycle(ctx context.Context) {
	// 获取活跃房间
	activeRooms, err := d.roomMgr.GetActiveRooms(ctx)
	if err != nil {
		logger.Error("Failed to get active rooms", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "dispatcher.pushBulletsCycle")
		return
	}

	// 如果没有活跃房间，直接返回
	if len(activeRooms) == 0 {
		return
	}

	// 获取每个房间的弹幕
	bulletsByRoom, err := d.bulletMgr.FetchBulletsByRoom(ctx, activeRooms, DefaultOptions().MaxBulletsPerRoom)
	if err != nil {
		logger.Error("Failed to fetch bullets by room", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "dispatcher.pushBulletsCycle")
		return
	}

	// 如果没有弹幕，直接返回
	if len(bulletsByRoom) == 0 {
		return
	}

	// 将弹幕发送给对应房间的客户端
	d.deliverBulletsToClients(ctx, bulletsByRoom)
}

// deliverBulletsToClients 将弹幕发送给对应房间的客户端
func (d *Dispatcher) deliverBulletsToClients(ctx context.Context, bulletsByRoom map[uint64][]*BulletData) {
	// 获取所有客户端
	clients := d.clientMgr.GetAllClients()

	// 对于每个客户端，找到其所在房间的弹幕并发送
	for _, client := range clients {
		// 获取该客户端所在房间的弹幕
		bullets, ok := bulletsByRoom[client.LiveID]
		if !ok || len(bullets) == 0 {
			continue
		}

		// 为这个客户端创建一个 span
		clientCtx, clientSpan := ob.StartSpan(ctx, "dispatcher.deliverToClient",
			trace.WithAttributes(
				attribute.Int64("user_id", int64(client.UserID)),
				attribute.Int64("live_id", int64(client.LiveID)),
			))

		// 将弹幕编码并发送给客户端
		if err := d.sendBulletsToClient(clientCtx, client, bullets); err != nil {
			logger.Warn("Failed to send bullets to client",
				zap.Uint64("userID", client.UserID),
				zap.String("userName", client.UserName),
				zap.Error(err))
			ob.RecordError(clientSpan, err, "dispatcher.deliverToClient")
			d.clientMgr.RemoveClient(clientCtx, client.UserID)
		}

		clientSpan.End()
	}
}

// sendBulletsToClient 将弹幕发送给单个客户端
func (d *Dispatcher) sendBulletsToClient(ctx context.Context, client *Client, bullets []*BulletData) error {
	startTime := time.Now()

	// 将弹幕消息列表编码为二进制数据
	data, err := d.bulletMgr.EncodeBullets(ctx, bullets)
	if err != nil {
		logger.Error("Failed to encode bullets for client",
			zap.Uint64("userID", client.UserID),
			zap.String("userName", client.UserName),
			zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "dispatcher.sendBulletsToClient")
		return err
	}

	// 发送数据给客户端
	if err := d.quicServer.WriteToStream(ctx, client.Stream, data); err != nil {
		return err
	}

	ob.BulletMessagesProcessed.WithLabelValues("dispatcher", "success").Inc()
	ob.RecordLatency(ctx, "dispatcher.sendBulletsToClient", time.Since(startTime))

	return nil
}

// Close 关闭弹幕分发器
func (d *Dispatcher) Close(spanCtx context.Context) {
	ctx, span := ob.StartSpan(spanCtx, "dispatcher.Close")
	defer span.End()

	startTime := time.Now()

	// 关闭 QUIC 服务器
	if err := d.quicServer.Close(); err != nil {
		logger.Error("Failed to close QUIC server", zap.Error(err))
		ob.RecordError(span, err, "dispatcher.Close")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "close").Inc()
	}

	// 关闭 Redis 客户端
	if err := d.redisClient.Close(); err != nil {
		logger.Error("Failed to close Redis client", zap.Error(err))
		ob.RecordError(span, err, "dispatcher.Close")
	}

	// 等待所有协程退出
	d.wg.Wait()

	ob.RecordLatency(ctx, "dispatcher.Close", time.Since(startTime))
	logger.Info("Dispatcher closed")
}
