package websocket

import (
	"context"
	"encoding/binary"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability" // 引入可观测性包
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute" // 用于添加 Span 属性
	"go.opentelemetry.io/otel/trace"     // 用于 Span 操作
	"go.uber.org/zap"
)

// BackDoorLiveRoomID 是一个特殊的直播间 ID，用于测试或调试目的
const BackDoorLiveRoomID = 10000

// Kafka key 字节切片的池
var kafkaKeyPool = pool.RegisterPool("kafka_key", func() []byte {
	return make([]byte, 8)
})

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
	// 创建根 Span 用于追踪整个服务启动
	ctx, span := observability.StartSpan(context.Background(), "gateway.Start",
		trace.WithAttributes(
			attribute.String("port", s.config.App.Port),
		))
	defer span.End()

	http.HandleFunc("/bullet", s.handleWebSocket)
	logger.Info("WebSocket gateway starting", zap.String("port", s.config.App.Port))
	if err := http.ListenAndServe(":"+s.config.App.Port, nil); err != nil {
		logger.Error("WebSocket gateway failed to start", zap.Error(err))
		observability.RecordError(span, err, "gateway.Start")
	}
	_ = ctx // 避免未使用警告，实际中 ctx 可用于更复杂的逻辑
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 创建 Span 追踪 WebSocket 处理
	ctx, span := observability.StartSpan(r.Context(), "gateway.handleWebSocket")
	defer span.End()

	// 接受 WebSocket 连接
	startTime := time.Now()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		observability.RecordError(span, err, "gateway.handleWebSocket")
		return
	}
	observability.RecordLatency(ctx, "websocket.Accept", time.Since(startTime))

	userID := uint64(0)
	liveID := uint64(0)
	s.pool.Add(userID, conn)

	defer func() {
		if liveID != 0 {
			s.unregisterLiveRoom(ctx, liveID)
		}
		conn.Close(websocket.StatusNormalClosure, "")
		s.pool.Remove(userID)
	}()

	for {
		// 读取消息
		_, data, err := conn.Read(ctx)
		if err != nil {
			logger.Warn("WebSocket read error", zap.Error(err))
			observability.RecordError(span, err, "gateway.handleWebSocket")
			return
		}

		// 创建子 Span 追踪消息处理
		msgCtx, msgSpan := observability.StartSpan(ctx, "gateway.processMessage",
			trace.WithAttributes(
				attribute.String("message_type", "unknown"),
				attribute.Int64("user_id", int64(userID)),
				attribute.Int64("live_id", int64(liveID)),
			))
		startTime = time.Now()

		msg, err := protocol.Decode(data)
		if err != nil {
			logger.Warn("Failed to decode message", zap.Error(err))
			observability.RecordError(msgSpan, err, "gateway.processMessage")
			msgSpan.End()
			continue
		}
		defer msg.Release()

		if userID == 0 {
			userID = msg.UserID
			s.pool.UpdateUserID(userID, conn)
			msgSpan.SetAttributes(attribute.Int64("user_id", int64(userID)))
		}
		msgSpan.SetAttributes(
			attribute.String("message_type", messageTypeToString(msg.Type)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		)

		switch msg.Type {
		case protocol.TypeBullet:
			logger.Info("Received bullet message",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName),
				zap.Uint64("liveID", msg.LiveID),
				zap.String("content", msg.Content))

			err = s.sendToKafka(msgCtx, msg)
			if err != nil {
				logger.Error("Failed to send message to Kafka", zap.Error(err))
				observability.RecordError(msgSpan, err, "gateway.processMessage")
			} else {
				logger.Info("Message sent to Kafka",
					zap.Uint64("userID", msg.UserID),
					zap.String("userName", msg.UserName),
					zap.Uint64("liveID", msg.LiveID))
			}

		case protocol.TypeHeartbeat:
			logger.Info("Received heartbeat",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName))

		case protocol.TypeCreateRoom:
			logger.Info("Received create room request",
				zap.Uint64("liveID", msg.LiveID),
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName))

			if exist, isBackdoor := s.isLiveRoomExists(msgCtx, msg.LiveID); exist && !isBackdoor {
				resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID)
				resp.Content = "Live room already exists"
				respData, err := resp.Encode(s.config.Performance.BulletCompression)
				if err != nil {
					logger.Error("Failed to encode error response", zap.Error(err))
					observability.RecordError(msgSpan, err, "gateway.processMessage")
					resp.Release()
					continue
				}
				conn.Write(msgCtx, websocket.MessageBinary, respData)
				conn.Close(websocket.StatusInvalidFramePayloadData, "Duplicate liveID")
				resp.Release()
				msgSpan.End()
				return
			}

			err = s.registerLiveRoom(msgCtx, msg.LiveID)
			if err != nil {
				logger.Error("Failed to register live room", zap.Error(err))
				observability.RecordError(msgSpan, err, "gateway.processMessage")
				continue
			}
			liveID = msg.LiveID

			resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID)
			respData, err := resp.Encode(s.config.Performance.BulletCompression)
			if err != nil {
				logger.Error("Failed to encode response", zap.Error(err))
				observability.RecordError(msgSpan, err, "gateway.processMessage")
				resp.Release()
				continue
			}
			err = conn.Write(msgCtx, websocket.MessageBinary, respData)
			if err != nil {
				logger.Error("Failed to send response", zap.Error(err))
				observability.RecordError(msgSpan, err, "gateway.processMessage")
				resp.Release()
				continue
			}
			logger.Info("Create room response sent",
				zap.Uint64("liveID", msg.LiveID),
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName))
			resp.Release()

		case protocol.TypeCheckRoom:
			logger.Info("Received check room request",
				zap.Uint64("liveID", msg.LiveID),
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName))

			exists, _ := s.isLiveRoomExists(msgCtx, msg.LiveID)
			content := "exists"
			if !exists {
				content = "not exists"
			}
			resp := protocol.NewCheckRoomMessage(msg.LiveID, msg.UserID)
			resp.Content = content
			respData, err := resp.Encode(s.config.Performance.BulletCompression)
			if err != nil {
				logger.Error("Failed to encode check room response", zap.Error(err))
				observability.RecordError(msgSpan, err, "gateway.processMessage")
				resp.Release()
				continue
			}
			err = conn.Write(msgCtx, websocket.MessageBinary, respData)
			if err != nil {
				logger.Error("Failed to send check room response", zap.Error(err))
				observability.RecordError(msgSpan, err, "gateway.processMessage")
				resp.Release()
				continue
			}
			logger.Info("Check room response sent",
				zap.Uint64("liveID", msg.LiveID),
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName),
				zap.String("result", content))
			resp.Release()
		}

		observability.RecordLatency(msgCtx, "gateway.processMessage", time.Since(startTime))
		msgSpan.End()
	}
}

func (s *Server) isLiveRoomExists(ctx context.Context, liveID uint64) (bool, bool) {
	// 创建 Span 追踪 Redis 检查
	ctx, span := observability.StartSpan(ctx, "gateway.isLiveRoomExists",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	if BackDoorLiveRoomID == liveID {
		return true, true
	}

	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	exists, err := s.redisClient.SIsMember(ctx, key, liveID).Result()
	if err != nil {
		logger.Error("Failed to check live room existence", zap.Uint64("liveID", liveID), zap.Error(err))
		observability.RecordError(span, err, "gateway.isLiveRoomExists")
		return true, false
	}
	observability.RecordLatency(ctx, "redis.SIsMember", time.Since(startTime))
	return exists, false
}

func (s *Server) registerLiveRoom(ctx context.Context, liveID uint64) error {
	// 创建 Span 追踪直播间注册
	ctx, span := observability.StartSpan(ctx, "gateway.registerLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	err := s.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		observability.RecordError(span, err, "gateway.registerLiveRoom")
		return err
	}
	err = s.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		observability.RecordError(span, err, "gateway.registerLiveRoom")
		return err
	}
	observability.RecordLatency(ctx, "redis.SAddAndExpire", time.Since(startTime))
	logger.Info("Live room registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

func (s *Server) unregisterLiveRoom(ctx context.Context, liveID uint64) {
	// 创建 Span 追踪直播间注销
	ctx, span := observability.StartSpan(ctx, "gateway.unregisterLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	err := s.redisClient.SRem(ctx, key, liveID).Err()
	if err != nil {
		logger.Error("Failed to unregister live room",
			zap.Uint64("liveID", liveID),
			zap.Error(err))
		observability.RecordError(span, err, "gateway.unregisterLiveRoom")
		return
	}
	observability.RecordLatency(ctx, "redis.SRem", time.Since(startTime))
	logger.Info("Live room unregistered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
}

func (s *Server) sendToKafka(ctx context.Context, msg *protocol.BulletMessage) error {
	// 创建 Span 追踪 Kafka 消息发送
	ctx, span := observability.StartSpan(ctx, "gateway.sendToKafka",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer span.End()

	startTime := time.Now()
	data, err := msg.Encode(s.config.Performance.BulletCompression)
	if err != nil {
		observability.RecordError(span, err, "gateway.sendToKafka")
		return err
	}

	key := kafkaKeyPool.Get()
	defer kafkaKeyPool.Put(key)

	binary.BigEndian.PutUint64(key, msg.LiveID)

	err = s.kafka.WriteMessages(ctx,
		kafka.Message{
			Key:   key,
			Value: data,
		},
	)
	if err != nil {
		observability.RecordError(span, err, "gateway.sendToKafka")
		return err
	}
	observability.RecordLatency(ctx, "kafka.WriteMessages", time.Since(startTime))
	return nil
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

// messageTypeToString 将消息类型转换为字符串，便于追踪
func messageTypeToString(t uint8) string {
	switch t {
	case protocol.TypeBullet:
		return "bullet"
	case protocol.TypeHeartbeat:
		return "heartbeat"
	case protocol.TypeCreateRoom:
		return "create_room"
	case protocol.TypeCheckRoom:
		return "check_room"
	default:
		return "unknown"
	}
}
