// internal/core/dispatcher/dispatcher.go
package dispatcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"strconv"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/penwyp/mini-edulive/config"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/quic-go/quic-go"
	"github.com/redis/go-redis/v9"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap"
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

func NewDispatcher(cfg *config.Config) (*Dispatcher, error) {
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		return nil, err
	}

	quicListener, err := quic.ListenAddr(cfg.Distributor.QUIC.Addr, generateTLSConfig(cfg.Distributor.QUIC), nil)
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

	d.mutex.Lock()
	d.clients[msg.UserID] = &ClientInfo{
		Stream:   stream,
		LiveID:   msg.LiveID,
		UserName: msg.Username,
	}
	d.mutex.Unlock()

	logger.Info("New client connected",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.Username))

	// 保持流活跃，直到连接关闭
	for {
		_, err := readStream(stream)
		if err != nil {
			logger.Warn("Stream read error, removing client",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.Username),
				zap.Error(err))
			d.removeClient(msg.UserID)
			return
		}
	}
}

func (d *Dispatcher) pushBulletLoop() {
	defer d.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
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

		// 压缩并推送
		compressedData, err := d.compressBullets(clientBullets)
		if err != nil {
			logger.Error("Failed to compress bullets for client",
				zap.Uint64("userID", userID),
				zap.String("userName", clientInfo.UserName),
				zap.Error(err))
			continue
		}

		err = d.sendToClient(clientInfo.Stream, compressedData)
		if err != nil {
			logger.Warn("Failed to send to client",
				zap.Uint64("userID", userID),
				zap.String("userName", clientInfo.UserName),
				zap.Error(err))
			d.removeClient(userID)
		}
	}
	d.mutex.RUnlock()

	logger.Debug("Bullets pushed",
		zap.Int("bullet_count", len(bullets)),
		zap.Duration("total_time", time.Since(startTime)))
}

func (d *Dispatcher) fetchTopBullets(ctx context.Context) ([]*protocol.BulletMessage, error) {
	activeRooms, err := d.redisClient.SMembers(ctx, d.keyBuilder.ActiveLiveRoomsKey()).Result()
	if err != nil {
		return nil, err
	}
	if len(activeRooms) == 0 {
		logger.Debug("No active live rooms found, sleeping for 10 second")
		time.Sleep(10 * time.Second)
		return nil, nil
	}

	var allBullets []*protocol.BulletMessage
	for _, roomStr := range activeRooms {
		liveID, err := strconv.ParseUint(roomStr, 10, 64)
		if err != nil {
			logger.Warn("Invalid liveID in active rooms", zap.String("liveID", roomStr), zap.Error(err))
			continue
		}

		rankingKey := d.keyBuilder.LiveRankingKey(liveID)
		users, err := d.redisClient.ZRevRangeWithScores(ctx, rankingKey, 0, 9999).Result()
		if err != nil {
			logger.Warn("Failed to fetch ranking for liveID", zap.Uint64("liveID", liveID), zap.Error(err))
			continue
		}

		bulletKey := d.keyBuilder.LiveBulletKey(liveID)
		for _, user := range users {
			userID := user.Member.(string)
			content, err := d.redisClient.LPop(ctx, bulletKey).Result()
			if err == redis.Nil {
				continue
			} else if err != nil {
				logger.Warn("Failed to fetch bullet for liveID", zap.Uint64("liveID", liveID), zap.Error(err))
				continue
			}

			allBullets = append(allBullets, &protocol.BulletMessage{
				Magic:      protocol.MagicNumber,
				Version:    protocol.CurrentVersion,
				Type:       protocol.TypeBullet,
				Timestamp:  time.Now().UnixMilli(),
				UserID:     parseUserID(userID),
				LiveID:     liveID,
				ContentLen: uint16(len(content)),
				Content:    content,
			})

			if len(allBullets) >= 10000 {
				break
			}
		}

		if len(allBullets) >= 10000 {
			break
		}
	}

	return allBullets, nil
}

func (d *Dispatcher) compressBullets(bullets []*protocol.BulletMessage) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}

	for _, bullet := range bullets {
		err = msgp.Encode(enc, bullet)
		if err != nil {
			enc.Close()
			return nil, err
		}
	}
	enc.Close()

	return buf.Bytes(), nil
}

func (d *Dispatcher) sendToClient(stream quic.Stream, data []byte) error {
	_, err := stream.Write(data)
	return err
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

func generateTLSConfig(quicConf config.QUIC) *tls.Config {
	cert, err := tls.LoadX509KeyPair(quicConf.CertFile, quicConf.KeyFile)
	if err != nil {
		logger.Panic("Failed to load TLS cert", zap.Error(err))
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-edulive"},
	}
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
