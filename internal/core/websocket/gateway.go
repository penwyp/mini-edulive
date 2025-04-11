// internal/core/websocket/gateway.go
package websocket

import (
	"context"
	"encoding/binary"
	"github.com/segmentio/kafka-go"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type Server struct {
	pool        *ConnPool
	config      *config.Config
	kafka       *kafka.Writer
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
		kafka:       pkgkafka.NewWriter(&cfg.Kafka),
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
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		return
	}

	// 添加连接到池中
	userID := uint64(0)
	liveID := uint64(0) // 用于记录创建的直播间 ID
	s.pool.Add(userID, conn)

	defer func() {
		// 客户端断开时，如果是创建者，移除直播间
		if liveID != 0 {
			s.unregisterLiveRoom(r.Context(), liveID)
		}
		conn.Close(websocket.StatusNormalClosure, "")
		s.pool.Remove(userID)
	}()

	for {
		_, data, err := conn.Read(r.Context())
		if err != nil {
			logger.Warn("WebSocket read error", zap.Error(err))
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

			// 检查直播间是否已存在
			if s.isLiveRoomExists(r.Context(), msg.LiveID) {
				// 返回错误消息并关闭连接
				resp := &protocol.BulletMessage{
					Magic:      protocol.MagicNumber,
					Version:    protocol.CurrentVersion,
					Type:       protocol.TypeCreateRoom,
					Timestamp:  time.Now().UnixMilli(),
					UserID:     msg.UserID,
					LiveID:     msg.LiveID,
					ContentLen: uint16(len("Live room already exists")),
					Content:    "Live room already exists",
				}
				respData, err := resp.Encode()
				if err != nil {
					logger.Error("Failed to encode error response", zap.Error(err))
					continue
				}
				conn.Write(r.Context(), websocket.MessageBinary, respData)
				conn.Close(websocket.StatusInvalidFramePayloadData, "Duplicate liveID")
				return
			}

			// 注册直播间
			err = s.registerLiveRoom(r.Context(), msg.LiveID)
			if err != nil {
				logger.Error("Failed to register live room", zap.Error(err))
				continue
			}
			liveID = msg.LiveID // 记录创建的 liveID

			// 回复成功消息
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

// isLiveRoomExists 检查直播间是否已存在
func (s *Server) isLiveRoomExists(ctx context.Context, liveID uint64) bool {
	key := s.keyBuilder.ActiveLiveRoomsKey()
	exists, err := s.redisClient.SIsMember(ctx, key, liveID).Result()
	if err != nil {
		logger.Error("Failed to check live room existence", zap.Uint64("liveID", liveID), zap.Error(err))
		return true // 假设存在以避免重复创建
	}
	return exists
}

// registerLiveRoom 注册活跃直播间到 Redis
func (s *Server) registerLiveRoom(ctx context.Context, liveID uint64) error {
	key := s.keyBuilder.ActiveLiveRoomsKey()
	err := s.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		return err
	}
	// 设置过期时间（24小时）
	err = s.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		return err
	}
	logger.Info("Live room registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

// unregisterLiveRoom 从 Redis 中移除活跃直播间
func (s *Server) unregisterLiveRoom(ctx context.Context, liveID uint64) {
	key := s.keyBuilder.ActiveLiveRoomsKey()
	err := s.redisClient.SRem(ctx, key, liveID).Err()
	if err != nil {
		logger.Error("Failed to unregister live room",
			zap.Uint64("liveID", liveID),
			zap.Error(err))
		return
	}
	logger.Info("Live room unregistered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
}

// sendToKafka 将消息发送到 Kafka
func (s *Server) sendToKafka(ctx context.Context, msg *protocol.BulletMessage) error {
	data, err := msg.Encode()
	if err != nil {
		return err
	}

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, msg.LiveID)

	err = s.kafka.WriteMessages(ctx,
		kafka.Message{
			Key:   key,
			Value: data,
		},
	)
	return err
}

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
