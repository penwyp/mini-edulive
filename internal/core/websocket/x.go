package websocket

import (
	"context"
	"encoding/binary"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
		ob.RedisOperations.WithLabelValues("connect", "failed").Inc()
	}
	return &Server{
		pool:        NewConnPool(cfg),
		config:      cfg,
		kafka:       pkgkafka.NewWriter(&cfg.Kafka),
		redisClient: redisClient,
		keyBuilder:  pkgcache.NewRedisKeyBuilder(),
	}
}

func (s *Server) Start(spanCtx context.Context) {
	ctx, span := ob.StartSpan(spanCtx, "gateway.Start",
		trace.WithAttributes(
			attribute.String("port", s.config.App.Port),
		))
	defer span.End()

	http.HandleFunc("/bullet", s.handleWebSocket)
	logger.Info("WebSocket gateway starting with Prometheus metrics endpoint", zap.String("port", s.config.App.Port))
	if err := http.ListenAndServe(":"+s.config.App.Port, nil); err != nil {
		logger.Error("WebSocket gateway failed to start", zap.Error(err))
		ob.RecordError(span, err, "gateway.Start")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "setup").Inc()
	}
	_ = ctx
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ctx, span := ob.StartSpan(r.Context(), "gateway.handleWebSocket")
	defer span.End()

	// 接受 WebSocket 连接
	startTime := time.Now()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleWebSocket")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "accept").Inc()
		return
	}
	ob.ActiveWebSocketConnections.WithLabelValues("gateway").Inc()
	ob.RecordLatency(ctx, "websocket.Accept", time.Since(startTime))

	userID := uint64(0)
	liveID := uint64(0)
	s.pool.Add(userID, conn)

	registerErr := s.registerLiveRoom(ctx, BackDoorLiveRoomID)
	if registerErr != nil {
		logger.Error("Failed to register backdoor live room", zap.Error(registerErr))
		ob.RecordError(span, registerErr, "gateway.handleWebSocket")
		return
	}

	defer func() {
		ob.ActiveWebSocketConnections.WithLabelValues("gateway").Dec()
		if liveID != 0 {
			s.unregisterLiveRoom(ctx, liveID)
		}
		startTime := time.Now()
		err := conn.Close(websocket.StatusNormalClosure, "")
		if err != nil {
			logger.Error("Failed to close WebSocket connection", zap.Error(err))
			ob.RecordError(span, err, "gateway.handleWebSocket")
			ob.WebSocketConnectionErrors.WithLabelValues("gateway", "close").Inc()
		}
		ob.RecordLatency(ctx, "websocket.Close", time.Since(startTime))
		s.pool.Remove(userID)
	}()

	// 创建消息通道和工作池
	msgChan := make(chan []byte, 100)     // 缓冲 100 条消息
	workerPool := make(chan struct{}, 10) // 最多 10 个并行工作者
	var wg sync.WaitGroup

	// 启动工作池
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for data := range msgChan {
				workerPool <- struct{}{} // 获取工作槽
				s.processMessage(ctx, conn, data, &userID, &liveID)
				<-workerPool // 释放工作槽
			}
		}()
	}

	// 主循环读取消息
	for {
		select {
		case <-ctx.Done():
			logger.Info("WebSocket context canceled, stopping...", zap.Error(ctx.Err()))
			close(msgChan)
			wg.Wait()
			return
		default:
			_, data, err := conn.Read(ctx)
			if err != nil {
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
					websocket.CloseStatus(err) == websocket.StatusGoingAway {
					logger.Info("WebSocket connection closed by client")
				} else {
					logger.Warn("WebSocket read error", zap.Error(err))
					ob.RecordError(span, err, "gateway.handleWebSocket")
					ob.WebSocketConnectionErrors.WithLabelValues("gateway", "read").Inc()
				}
				close(msgChan)
				wg.Wait()
				return
			}

			// 将消息放入通道
			select {
			case msgChan <- data:
				ob.WebSocketMessagesReceived.WithLabelValues("gateway").Inc()
			default:
				logger.Warn("Message channel full, dropping message")
				ob.WebSocketMessagesDropped.WithLabelValues("gateway").Inc()
			}
		}
	}
}

func (s *Server) processMessage(ctx context.Context, conn *websocket.Conn, data []byte, userID, liveID *uint64) {
	msgCtx, msgSpan := ob.StartSpan(ctx, "gateway.processMessage",
		trace.WithAttributes(
			attribute.String("message_type", "unknown"),
			attribute.Int64("user_id", int64(*userID)),
			attribute.Int64("live_id", int64(*liveID)),
		))
	defer msgSpan.End()

	startTime := time.Now()

	msg, err := protocol.Decode(data)
	if err != nil {
		logger.Warn("Failed to decode message", zap.Error(err))
		ob.RecordError(msgSpan, err, "gateway.processMessage")
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return
	}
	defer msg.Release()

	if *userID == 0 {
		*userID = msg.UserID
		s.pool.UpdateUserID(*userID, conn)
		msgSpan.SetAttributes(attribute.Int64("user_id", int64(*userID)))
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
			ob.RecordError(msgSpan, err, "gateway.processMessage")
			ob.BulletMessagesProcessed.WithLabelValues("gateway", "failed").Inc()
		} else {
			logger.Info("Message sent to Kafka",
				zap.Uint64("userID", msg.UserID),
				zap.String("userName", msg.UserName),
				zap.Uint64("liveID", msg.LiveID),
				zap.String("content", msg.Content))
			ob.BulletMessagesProcessed.WithLabelValues("gateway", "success").Inc()
		}

	case protocol.TypeHeartbeat:
		logger.Info("Received heartbeat",
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName))
		ob.HeartbeatFailures.WithLabelValues("gateway").Inc()

	case protocol.TypeCreateRoom:
		logger.Info("Received create room request",
			zap.Uint64("liveID", msg.LiveID),
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName))

		if exist, isBackdoor := s.isLiveRoomExists(msgCtx, msg.LiveID); exist && !isBackdoor {
			ob.LiveRoomCreations.WithLabelValues("failed").Inc()

			resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID, msg.UserName)
			resp.Content = "Live room already exists"
			respData, err := resp.Encode(s.config.Performance.BulletCompression)
			if err != nil {
				logger.Error("Failed to encode error response", zap.Error(err))
				ob.RecordError(msgSpan, err, "gateway.processMessage")
				ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
				ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
				resp.Release()
				return
			}
			err = conn.Write(msgCtx, websocket.MessageBinary, respData)
			if err != nil {
				logger.Error("Failed to send error response", zap.Error(err))
				ob.RecordError(msgSpan, err, "gateway.processMessage")
				ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
				resp.Release()
				return
			}
			err = conn.Close(websocket.StatusInvalidFramePayloadData, "Duplicate liveID")
			if err != nil {
				logger.Error("Failed to close WebSocket connection", zap.Error(err))
				ob.RecordError(msgSpan, err, "gateway.processMessage")
				ob.WebSocketConnectionErrors.WithLabelValues("gateway", "close").Inc()
			}
			resp.Release()
			return
		}

		err = s.registerLiveRoom(msgCtx, msg.LiveID)
		if err != nil {
			logger.Error("Failed to register live room", zap.Error(err))
			ob.RecordError(msgSpan, err, "gateway.processMessage")
			ob.LiveRoomCreations.WithLabelValues("failed").Inc()
			return
		}

		ob.LiveRoomCreations.WithLabelValues("success").Inc()
		*liveID = msg.LiveID

		resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID, msg.UserName)
		respData, err := resp.Encode(s.config.Performance.BulletCompression)
		if err != nil {
			logger.Error("Failed to encode response", zap.Error(err))
			ob.RecordError(msgSpan, err, "gateway.processMessage")
			ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
			ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
			resp.Release()
			return
		}
		err = conn.Write(msgCtx, websocket.MessageBinary, respData)
		if err != nil {
			logger.Error("Failed to send response", zap.Error(err))
			ob.RecordError(msgSpan, err, "gateway.processMessage")
			ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
			resp.Release()
			return
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
			content = "not_exists"
		}
		ob.LiveRoomChecks.WithLabelValues(content).Inc()
		resp := protocol.NewCheckRoomMessage(msg.LiveID, msg.UserID)
		resp.Content = content
		respData, err := resp.Encode(s.config.Performance.BulletCompression)
		if err != nil {
			logger.Error("Failed to encode check room response", zap.Error(err))
			ob.RecordError(msgSpan, err, "gateway.processMessage")
			ob.ProtocolParseErrors.WithLabelValues("check_room").Inc()
			ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
			resp.Release()
			return
		}
		err = conn.Write(msgCtx, websocket.MessageBinary, respData)
		if err != nil {
			logger.Error("Failed to send check room response", zap.Error(err))
			ob.RecordError(msgSpan, err, "gateway.processMessage")
			ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
			resp.Release()
			return
		}
		logger.Info("Check room response sent",
			zap.Uint64("liveID", msg.LiveID),
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName),
			zap.String("result", content))
		resp.Release()
	}

	ob.RecordLatency(msgCtx, "gateway.processMessage", time.Since(startTime))
	ob.BulletMessageLatency.WithLabelValues("gateway", messageTypeToString(msg.Type)).Observe(time.Since(startTime).Seconds())
}

func (s *Server) isLiveRoomExists(ctx context.Context, liveID uint64) (bool, bool) {
	ctx, span := ob.StartSpan(ctx, "gateway.isLiveRoomExists",
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
		ob.RecordError(span, err, "gateway.isLiveRoomExists")
		ob.RedisOperations.WithLabelValues("sismember", "failed").Inc()
		return true, false
	}
	ob.RecordLatency(ctx, "redis.SIsMember", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("sismember", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("sismember").Observe(time.Since(startTime).Seconds())
	return exists, false
}

func (s *Server) registerLiveRoom(ctx context.Context, liveID uint64) error {
	ctx, span := ob.StartSpan(ctx, "gateway.registerLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	err := s.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		ob.RecordError(span, err, "gateway.registerLiveRoom")
		ob.RedisOperations.WithLabelValues("sadd", "failed").Inc()
		return err
	}
	err = s.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		ob.RecordError(span, err, "gateway.registerLiveRoom")
		ob.RedisOperations.WithLabelValues("expire", "failed").Inc()
		return err
	}
	ob.RecordLatency(ctx, "redis.SAddAndExpire", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("sadd", "success").Inc()
	ob.RedisOperations.WithLabelValues("expire", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("sadd").Observe(time.Since(startTime).Seconds())
	logger.Info("Live room registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

func (s *Server) unregisterLiveRoom(ctx context.Context, liveID uint64) {
	ctx, span := ob.StartSpan(ctx, "gateway.unregisterLiveRoom",
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
		ob.RecordError(span, err, "gateway.unregisterLiveRoom")
		ob.RedisOperations.WithLabelValues("srem", "failed").Inc()
		return
	}
	ob.RecordLatency(ctx, "redis.SRem", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("srem", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("srem").Observe(time.Since(startTime).Seconds())
	logger.Info("Live room unregistered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
}

func (s *Server) sendToKafka(ctx context.Context, msg *protocol.BulletMessage) error {
	ctx, span := ob.StartSpan(ctx, "gateway.sendToKafka",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer span.End()

	startTime := time.Now()
	data, err := msg.Encode(s.config.Performance.BulletCompression)
	if err != nil {
		ob.RecordError(span, err, "gateway.sendToKafka")
		ob.KafkaMessagesSent.WithLabelValues("failed").Inc()
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
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
		ob.RecordError(span, err, "gateway.sendToKafka")
		ob.KafkaMessagesSent.WithLabelValues("failed").Inc()
		return err
	}
	ob.RecordLatency(ctx, "kafka.WriteMessages", time.Since(startTime))
	ob.KafkaMessagesSent.WithLabelValues("success").Inc()
	ob.KafkaMessageLatency.WithLabelValues().Observe(time.Since(startTime).Seconds())
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
