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

func (s *Server) Start() {
	http.HandleFunc("/bullet", s.handleWebSocket)
	logger.Info("WebSocket gateway starting", zap.String("port", s.config.App.Port))
	if err := http.ListenAndServe(":"+s.config.App.Port, nil); err != nil {
		logger.Error("WebSocket gateway failed to start", zap.Error(err))
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		return
	}

	userID := uint64(0)
	liveID := uint64(0)
	s.pool.Add(userID, conn)

	defer func() {
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

		msg, err := protocol.Decode(data)
		if err != nil {
			logger.Warn("Failed to decode message", zap.Error(err))
			continue
		}

		if userID == 0 {
			userID = msg.UserID
			s.pool.UpdateUserID(userID, conn)
		}

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

			if s.isLiveRoomExists(r.Context(), msg.LiveID) {
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

			err = s.registerLiveRoom(r.Context(), msg.LiveID)
			if err != nil {
				logger.Error("Failed to register live room", zap.Error(err))
				continue
			}
			liveID = msg.LiveID

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

		case protocol.TypeCheckRoom:
			logger.Info("Received check room request",
				zap.Uint64("userID", msg.UserID),
				zap.Uint64("liveID", msg.LiveID))

			exists := s.isLiveRoomExists(r.Context(), msg.LiveID)
			content := "exists"
			if !exists {
				content = "not exists"
			}
			resp := &protocol.BulletMessage{
				Magic:      protocol.MagicNumber,
				Version:    protocol.CurrentVersion,
				Type:       protocol.TypeCheckRoom,
				Timestamp:  time.Now().UnixMilli(),
				UserID:     msg.UserID,
				LiveID:     msg.LiveID,
				ContentLen: uint16(len(content)),
				Content:    content,
			}
			respData, err := resp.Encode()
			if err != nil {
				logger.Error("Failed to encode check room response", zap.Error(err))
				continue
			}
			err = conn.Write(r.Context(), websocket.MessageBinary, respData)
			if err != nil {
				logger.Error("Failed to send check room response", zap.Error(err))
				continue
			}
			logger.Info("Check room response sent",
				zap.Uint64("userID", msg.UserID),
				zap.Uint64("liveID", msg.LiveID),
				zap.String("result", content))
		}
	}
}

func (s *Server) isLiveRoomExists(ctx context.Context, liveID uint64) bool {
	key := s.keyBuilder.ActiveLiveRoomsKey()
	exists, err := s.redisClient.SIsMember(ctx, key, liveID).Result()
	if err != nil {
		logger.Error("Failed to check live room existence", zap.Uint64("liveID", liveID), zap.Error(err))
		return true // 默认假设存在以避免误操作
	}
	return exists
}

func (s *Server) registerLiveRoom(ctx context.Context, liveID uint64) error {
	key := s.keyBuilder.ActiveLiveRoomsKey()
	err := s.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		return err
	}
	err = s.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		return err
	}
	logger.Info("Live room registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

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
