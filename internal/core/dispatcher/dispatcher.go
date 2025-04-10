package dispatcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"golang.org/x/net/quic"
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
	quicListener quic.Listener
	clients      map[uint64]quic.Stream // 客户端连接映射
	mutex        sync.RWMutex
	wg           sync.WaitGroup
}

func NewDispatcher(cfg *config.Config) (*Dispatcher, error) {
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		return nil, err
	}

	// 初始化 QUIC 监听器
	quicListener, err := quic.ListenAddr(cfg.Distributor.QUIC.Addr, generateTLSConfig(), nil)
	if err != nil {
		logger.Error("Failed to start QUIC listener", zap.Error(err))
		return nil, err
	}

	return &Dispatcher{
		config:       cfg,
		redisClient:  redisClient,
		keyBuilder:   pkgcache.NewRedisKeyBuilder(),
		quicListener: quicListener,
		clients:      make(map[uint64]quic.Stream),
	}, nil
}

func (d *Dispatcher) Start() {
	logger.Info("Dispatcher started", zap.String("quic_addr", d.config.Distributor.QUIC.Addr))

	// 接受客户端连接
	d.wg.Add(1)
	go d.acceptConnections()

	// 定时推送弹幕
	d.wg.Add(1)
	go d.pushBulletLoop()

	d.wg.Wait()
}

func (d *Dispatcher) acceptConnections() {
	defer d.wg.Done()
	for {
		session, err := d.quicListener.Accept(context.Background())
		if err != nil {
			logger.Error("Failed to accept QUIC connection", zap.Error(err))
			return
		}

		go d.handleSession(session)
	}
}

func (d *Dispatcher) handleSession(session quic.Session) {
	stream, err := session.AcceptStream(context.Background())
	if err != nil {
		logger.Error("Failed to accept QUIC stream", zap.Error(err))
		return
	}

	// 假设客户端首次发送消息包含 UserID
	data, err := readStream(stream)
	if err != nil {
		logger.Error("Failed to read initial message", zap.Error(err))
		return
	}

	msg, err := protocol.Decode(data)
	if err != nil {
		logger.Error("Failed to decode initial message", zap.Error(err))
		return
	}

	d.mutex.Lock()
	d.clients[msg.UserID] = stream
	d.mutex.Unlock()

	logger.Info("New client connected", zap.Uint64("userID", msg.UserID))
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

	// 从 Redis 获取 Top 10000 弹幕
	bullets, err := d.fetchTopBullets(ctx)
	if err != nil {
		logger.Error("Failed to fetch top bullets", zap.Error(err))
		return
	}

	// 序列化并压缩
	compressedData, err := d.compressBullets(bullets)
	if err != nil {
		logger.Error("Failed to compress bullets", zap.Error(err))
		return
	}

	// 分发给所有客户端
	d.mutex.RLock()
	for userID, stream := range d.clients {
		err := d.sendToClient(stream, compressedData)
		if err != nil {
			logger.Warn("Failed to send to client", zap.Uint64("userID", userID), zap.Error(err))
			d.removeClient(userID)
		}
	}
	d.mutex.RUnlock()

	logger.Debug("Bullets pushed",
		zap.Int("bullet_count", len(bullets)),
		zap.Duration("total_time", time.Since(startTime)))
}

func (d *Dispatcher) fetchTopBullets(ctx context.Context) ([]*protocol.BulletMessage, error) {
	// 这里假设从所有直播间获取 Top 10000，实际可能需要按 LiveID 分组
	rankingKey := d.keyBuilder.LiveRankingKey(0) // 示例：全局排行榜
	users, err := d.redisClient.ZRevRangeWithScores(ctx, rankingKey, 0, 9999).Result()
	if err != nil {
		return nil, err
	}

	var bullets []*protocol.BulletMessage
	for _, user := range users {
		userID := user.Member.(string)
		bulletKey := d.keyBuilder.LiveBulletKey(0) // 示例：全局弹幕池
		content, err := d.redisClient.LPop(ctx, bulletKey).Result()
		if err == redis.Nil {
			continue
		} else if err != nil {
			return nil, err
		}

		bullets = append(bullets, &protocol.BulletMessage{
			Magic:      protocol.MagicNumber,
			Version:    protocol.CurrentVersion,
			Type:       protocol.TypeBullet,
			Timestamp:  time.Now().UnixMilli(),
			UserID:     parseUserID(userID),
			LiveID:     0, // 示例值
			ContentLen: uint16(len(content)),
			Content:    content,
		})
	}

	return bullets, nil
}

func (d *Dispatcher) compressBullets(bullets []*protocol.BulletMessage) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}

	// 使用 msgp 序列化（更高效）
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
	delete(d.clients, userID)
	d.mutex.Unlock()
}

func (d *Dispatcher) Close() {
	d.quicListener.Close()
	d.redisClient.Close()
	d.wg.Wait()
	logger.Info("Dispatcher closed")
}

// 辅助函数：生成 TLS 配置（QUIC 必需）
func generateTLSConfig() *tls.Config {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		logger.Panic("Failed to load TLS cert", zap.Error(err))
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-edulive"},
	}
}

// 辅助函数：读取 QUIC Stream 数据
func readStream(stream quic.Stream) ([]byte, error) {
	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// 辅助函数：解析 UserID
func parseUserID(userIDStr string) uint64 {
	id, _ := strconv.ParseUint(userIDStr, 10, 64)
	return id
}
