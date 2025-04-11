package websocket

import (
	"context"
	"encoding/binary"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.uber.org/zap"
)

// Server WebSocket 服务端
type Server struct {
	pool        *ConnPool
	config      *config.Config
	kafka       *kafka.Writer // Kafka 生产者
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewServer 创建 WebSocket 服务端
func NewServer(cfg *config.Config) *Server {
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Panic("Failed to create Redis client", zap.Error(err))
	}
	return &Server{
		pool:        NewConnPool(cfg),
		config:      cfg,
		kafka:       pkgkafka.NewWriter(&cfg.Kafka), // 使用新抽取的函数,
		redisClient: redisClient,
		keyBuilder:  pkgcache.NewRedisKeyBuilder(),
	}
}

// Start 启动 WebSocket 服务
func (s *Server) Start() {
	http.HandleFunc("/bullet", s.handleWebSocket)
	logger.Info("WebSocket gateway starting", zap.String("port", s.config.App.Port))
	if err := http.ListenAndServe(":"+s.config.App.Port, nil); err != nil {
		logger.Error("WebSocket gateway failed to start", zap.Error(err))
	}
}

// handleWebSocket 处理 WebSocket 连接
// handleWebSocket 处理 WebSocket 连接
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// 添加连接到池中
	userID := uint64(0)
	s.pool.Add(userID, conn)

	for {
		_, data, err := conn.Read(r.Context())
		if err != nil {
			logger.Warn("WebSocket read error", zap.Error(err))
			s.pool.Remove(userID)
			return
		}

		// 解析二进制消息
		msg, err := protocol.Decode(data)
		if err != nil {
			logger.Warn("Failed to decode message", zap.Error(err))
			continue
		}

		// 更新用户ID
		if userID == 0 {
			userID = msg.UserID
			s.pool.UpdateUserID(userID, conn)
		}

		// 处理消息
		switch msg.Type {
		case protocol.TypeBullet:
			logger.Info("Received bullet message",
				zap.Uint64("userID", msg.UserID),
				zap.Uint64("liveID", msg.LiveID),
				zap.String("content", msg.Content))

			// 转发到 Kafka
			err = s.sendToKafka(r.Context(), msg)
			if err != nil {
				logger.Error("Failed to send message to Kafka", zap.Error(err))
				continue
			}
			logger.Info("Message sent to Kafka",
				zap.Uint64("userID", msg.UserID),
				zap.Uint64("liveID", msg.LiveID))

		case protocol.TypeHeartbeat:
			logger.Info("Received heartbeat", zap.Uint64("userID", msg.UserID))

		case protocol.TypeCreateRoom:
			logger.Info("Received create room request",
				zap.Uint64("userID", msg.UserID),
				zap.Uint64("liveID", msg.LiveID))

			// 记录活跃直播间到 Redis
			err = s.registerLiveRoom(r.Context(), msg.LiveID)
			if err != nil {
				logger.Error("Failed to register live room", zap.Error(err))
				continue
			}

			// 回复确认消息
			resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID)
			respData, err := resp.Encode()
			if err != nil {
				logger.Error("Failed to encode response", zap.Error(err))
				continue
			}
			err = conn.Write(r.Context(), websocket.MessageBinary, respData)
			if err != nil {
				logger.Error("Failed to send response", zap.Error(err))
				continue
			}
			logger.Info("Create room response sent",
				zap.Uint64("userID", msg.UserID),
				zap.Uint64("liveID", msg.LiveID))
		}
	}
}

// registerLiveRoom 记录活跃直播间到 Redis
func (s *Server) registerLiveRoom(ctx context.Context, liveID uint64) error {
	key := s.keyBuilder.ActiveLiveRoomsKey()
	err := s.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		return err
	}
	// 设置过期时间（例如 24 小时）
	err = s.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		return err
	}
	logger.Info("Live room registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

// sendToKafka 将消息发送到 Kafka，按照 LiveID 分区
func (s *Server) sendToKafka(ctx context.Context, msg *protocol.BulletMessage) error {
	data, err := msg.Encode()
	if err != nil {
		return err
	}

	// 使用 LiveID 作为 Key，确保同一直播间的消息分配到同一分区
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, msg.LiveID)

	err = s.kafka.WriteMessages(ctx,
		kafka.Message{
			Key:   key,  // 使用 LiveID 作为分区键
			Value: data, // 二进制消息内容
		},
	)
	return err
}

// ConnPool 连接池
type ConnPool struct {
	sync.RWMutex
	conns  map[uint64]*websocket.Conn
	config *config.Config
}

func NewConnPool(cfg *config.Config) *ConnPool {
	return &ConnPool{
		conns:  make(map[uint64]*websocket.Conn),
		config: cfg,
	}
}

func (p *ConnPool) Add(userID uint64, conn *websocket.Conn) {
	p.Lock()
	defer p.Unlock()
	p.conns[userID] = conn
}

func (p *ConnPool) Remove(userID uint64) {
	p.Lock()
	defer p.Unlock()
	delete(p.conns, userID)
}

func (p *ConnPool) UpdateUserID(userID uint64, conn *websocket.Conn) {
	p.Lock()
	defer p.Unlock()
	p.conns[userID] = conn
}

func getBalancer(balanceMethod string) kafka.Balancer {
	switch balanceMethod {
	case "hash":
		return &kafka.Hash{}
	case "referencehash":
		return &kafka.ReferenceHash{}
	case "roundrobin":
		return &kafka.RoundRobin{}
	case "murmur2":
		return &kafka.Murmur2Balancer{}
	case "crc32":
		return &kafka.CRC32Balancer{}
	default:
		return &kafka.Hash{} // 默认使用 Hash 分区策略
	}
}
