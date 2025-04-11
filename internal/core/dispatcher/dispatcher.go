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
	"github.com/penwyp/mini-edulive/internal/core/websocket"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/quic-go/quic-go"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// BulletMessage 切片的池，初始容量为 100
var bulletSlicePool = pool.RegisterPool("bullet_slice", func() []*protocol.BulletMessage {
	return make([]*protocol.BulletMessage, 0, 100)
})

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

func NewDispatcher(cfg *config.Config) (*Dispatcher, error) {
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		return nil, err
	}

	quicListener, err := quic.ListenAddr(cfg.Distributor.QUIC.Addr, util.GenerateTLSConfig(cfg.Distributor.QUIC.CertFile, cfg.Distributor.QUIC.KeyFile), nil)
	if err != nil {
		logger.Error("Failed to start QUIC listener", zap.Error(err))
		return nil, err
	}

	return &Dispatcher{
		config:       cfg,
		redisClient:  redisClient,
		keyBuilder:   pkgcache.NewRedisKeyBuilder(),
		quicListener: quicListener,
		clients:      make(map[uint64]*ClientInfo),
	}, nil
}

func (d *Dispatcher) Start() {
	logger.Info("Dispatcher started", zap.String("quic_addr", d.config.Distributor.QUIC.Addr))

	d.wg.Add(1)
	go d.acceptConnections()

	d.wg.Add(1)
	go d.pushBulletLoop()

	d.wg.Wait()
}

func (d *Dispatcher) acceptConnections() {
	defer d.wg.Done()
	for {
		conn, err := d.quicListener.Accept(context.Background())
		if err != nil {
			logger.Error("Failed to accept QUIC connection", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		go d.handleConnection(conn)
	}
}

func (d *Dispatcher) handleConnection(conn quic.Connection) {
	defer conn.CloseWithError(0, "connection closed")

	stream, err := conn.AcceptStream(context.Background())
	if err != nil {
		logger.Error("Failed to accept QUIC stream", zap.Error(err))
		return
	}

	// 读取初始化消息以获取 userID 和 liveID
	data, err := readStream(stream)
	if err != nil {
		logger.Error("Failed to read initial message", zap.Error(err))
		stream.Close()
		return
	}

	msg, err := protocol.Decode(data)
	if err != nil {
		logger.Error("Failed to decode initial message", zap.Error(err))
		stream.Close()
		return
	}
	defer msg.Release() // 确保归还

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

	// 保持流活跃，直到连接关闭
	for {
		_, err := readStream(stream)
		if err != nil {
			logger.Warn("Stream read error, removing client",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName),
				zap.Error(err))
			d.removeClient(msg.UserID)
			return
		}
	}
}

func (d *Dispatcher) pushBulletLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		d.pushBullets()
	}
}

func (d *Dispatcher) pushBullets() {
	ctx := context.Background()
	startTime := time.Now()

	bullets, err := d.fetchTopBullets(ctx)
	if err != nil {
		logger.Error("Failed to fetch top bullets", zap.Error(err))
		return
	}

	if len(bullets) == 0 {
		bulletSlicePool.Put(bullets) // 无弹幕时归还切片
		return
	}

	// 按 liveID 分组弹幕
	bulletsByLiveID := make(map[uint64][]*protocol.BulletMessage)
	for _, bullet := range bullets {
		bulletsByLiveID[bullet.LiveID] = append(bulletsByLiveID[bullet.LiveID], bullet)
	}

	d.mutex.RLock()
	for userID, clientInfo := range d.clients {
		// 获取该客户端订阅的直播间弹幕
		clientBullets, ok := bulletsByLiveID[clientInfo.LiveID]
		if !ok || len(clientBullets) == 0 {
			continue
		}

		// 编码弹幕消息
		data, err := d.encodeBullets(clientBullets)
		if err != nil {
			logger.Error("Failed to encode bullets for client",
				zap.Uint64("userID", userID),
				zap.String("userName", clientInfo.UserName),
				zap.Error(err))
			continue
		}

		err = d.sendToClient(clientInfo.Stream, data)
		if err != nil {
			logger.Warn("Failed to send to client",
				zap.Uint64("userID", userID),
				zap.String("userName", clientInfo.UserName),
				zap.Error(err))
			d.removeClient(userID)
		}
	}
	d.mutex.RUnlock()

	// 归还所有 BulletMessage 和切片
	for _, bullet := range bullets {
		bullet.Release()
	}
	bulletSlicePool.Put(bullets)

	logger.Debug("Bullets pushed",
		zap.Int("bullet_count", len(bullets)),
		zap.Duration("total_time", time.Since(startTime)))
}

// encodeBullets 编码弹幕消息
func (d *Dispatcher) encodeBullets(bullets []*protocol.BulletMessage) ([]byte, error) {
	// 从池中获取 bytes.Buffer
	bufPool, err := pool.GetPool[*bytes.Buffer]("bytes_buffer")
	if err != nil {
		return nil, fmt.Errorf("failed to get bytes_buffer pool: %v", err)
	}
	buf := bufPool.Get()
	buf.Reset()            // 清空缓冲区
	defer bufPool.Put(buf) // 使用后归还

	for _, bullet := range bullets {
		data, err := bullet.Encode(d.config.Performance.BulletCompression)
		if err != nil {
			logger.Warn("Failed to encode bullet", zap.Error(err))
			continue
		}
		// 写入消息长度（4 字节）+ 消息内容
		if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
			return nil, err
		}
		buf.Write(data)
	}
	logger.Debug("Encoded bullets", zap.Int("bytes", buf.Len()))
	return buf.Bytes(), nil
}

func (d *Dispatcher) sendToClient(stream quic.Stream, data []byte) error {
	n, err := stream.Write(data)
	logger.Debug("Sent to client", zap.Int("bytes", n), zap.Error(err))
	return err
}

func (d *Dispatcher) fetchTopBullets(ctx context.Context) ([]*protocol.BulletMessage, error) {
	// 从池中获取切片
	allBullets := bulletSlicePool.Get()
	allBullets = allBullets[:0] // 清空切片

	activeRooms, err := d.redisClient.SMembers(ctx, d.keyBuilder.ActiveLiveRoomsKey()).Result()
	if err != nil {
		bulletSlicePool.Put(allBullets) // 错误时归还
		return nil, err
	}
	if len(activeRooms) == 0 {
		activeRooms = []string{util.FormatUint64ToString(websocket.BackDoorLiveRoomID)}
	}

	for _, roomStr := range activeRooms {
		liveID, err := strconv.ParseUint(roomStr, 10, 64)
		if err != nil {
			logger.Warn("Invalid liveID in active rooms", zap.String("liveID", roomStr), zap.Error(err))
			continue
		}
		bulletKey := d.keyBuilder.LiveBulletKey(liveID)

		// 从 Redis 获取序列化的弹幕数据
		serializedContent, err := d.redisClient.LPop(ctx, bulletKey).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			logger.Warn("Failed to fetch bullet for liveID", zap.Uint64("liveID", liveID), zap.Error(err))
			continue
		}

		// 反序列化 JSON 数据
		var serializedBullet protocol.SerializedBullet
		if err := json.Unmarshal([]byte(serializedContent), &serializedBullet); err != nil {
			logger.Warn("Failed to unmarshal serialized bullet",
				zap.String("serialized_content", serializedContent),
				zap.Error(err))
			continue
		}

		// 从池中获取 BulletMessage
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

	return allBullets, nil
}

func (d *Dispatcher) removeClient(userID uint64) {
	d.mutex.Lock()
	if clientInfo, ok := d.clients[userID]; ok {
		clientInfo.Stream.Close()
		delete(d.clients, userID)
	}
	d.mutex.Unlock()
}

func (d *Dispatcher) Close() {
	d.quicListener.Close()
	d.redisClient.Close()
	d.wg.Wait()
	logger.Info("Dispatcher closed")
}

func readStream(stream quic.Stream) ([]byte, error) {
	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func parseUserID(userIDStr string) uint64 {
	id, _ := strconv.ParseUint(userIDStr, 10, 64)
	return id
}
