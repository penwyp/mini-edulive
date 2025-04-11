package websocket

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Server 是WebSocket服务器的主结构体
type Server struct {
	connManager  *ConnectionManager
	messageQueue *MessageQueue
	handler      *MessageHandler
	config       *config.Config
	kafka        *kafka.Writer
	redisClient  *redis.ClusterClient
	keyBuilder   *pkgcache.RedisKeyBuilder
}

// NewServer 创建一个新的WebSocket服务器实例
func NewServer(cfg *config.Config) *Server {
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Panic("Failed to create Redis client", zap.Error(err))
		ob.RedisOperations.WithLabelValues("connect", "failed").Inc()
	}
	ob.RedisOperations.WithLabelValues("connect", "success").Inc()

	kafkaWriter := pkgkafka.NewWriter(&cfg.Kafka)
	keyBuilder := pkgcache.NewRedisKeyBuilder()
	roomManager := NewRoomManager(redisClient, keyBuilder)
	messageProcessor := NewMessageProcessor(kafkaWriter, roomManager, cfg)
	messageHandler := NewMessageHandler(messageProcessor, cfg)
	connManager := NewConnectionManager()
	messageQueue := NewMessageQueue(DefaultQueueOptions())

	return &Server{
		connManager:  connManager,
		messageQueue: messageQueue,
		handler:      messageHandler,
		config:       cfg,
		kafka:        kafkaWriter,
		redisClient:  redisClient,
		keyBuilder:   keyBuilder,
	}
}

// Start 启动WebSocket服务器
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

// handleWebSocket 处理新的WebSocket连接
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ctx, span := ob.StartSpan(r.Context(), "gateway.handleWebSocket")
	defer span.End()

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

	connCtx := NewConnectionContext()

	if err := s.handler.RegisterBackdoorRoom(ctx); err != nil {
		logger.Error("Failed to register backdoor live room", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleWebSocket")
		conn.Close(websocket.StatusInternalError, "Failed to register backdoor room")
		return
	}

	defer func() {
		ob.ActiveWebSocketConnections.WithLabelValues("gateway").Dec()
		if connCtx.LiveID != 0 {
			s.handler.UnregisterRoom(ctx, connCtx.LiveID)
		}
		startTime := time.Now()
		if err := conn.Close(websocket.StatusNormalClosure, ""); err != nil {
			logger.Error("Failed to close WebSocket connection", zap.Error(err))
			ob.RecordError(span, err, "gateway.handleWebSocket")
			ob.WebSocketConnectionErrors.WithLabelValues("gateway", "close").Inc()
		}
		ob.RecordLatency(ctx, "websocket.Close", time.Since(startTime))
		s.connManager.Remove(connCtx.UserID)
	}()

	s.connManager.Add(connCtx.UserID, conn)
	done := s.messageQueue.StartWorkers(ctx, conn, connCtx, s.handler)

	for {
		select {
		case <-ctx.Done():
			logger.Info("WebSocket context canceled, stopping...", zap.Error(ctx.Err()))
			<-done
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
				<-done
				return
			}

			msg, err := protocol.Decode(data)
			if err != nil {
				logger.Warn("Failed to decode message", zap.Error(err))
				ob.RecordError(span, err, "gateway.handleWebSocket")
				ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
				continue
			}
			defer msg.Release()

			if connCtx.UserID == 0 && msg.UserID != 0 {
				connCtx.UserID = msg.UserID
				s.connManager.UpdateUserID(connCtx.UserID, conn)
			}
			if connCtx.LiveID == 0 && msg.LiveID != 0 {
				connCtx.LiveID = msg.LiveID
			}

			if !s.messageQueue.EnqueueMessage(data) {
				logger.Warn("Message channel full, dropping message")
				ob.WebSocketMessagesDropped.WithLabelValues("gateway").Inc()
				continue
			}

			ob.WebSocketMessagesReceived.WithLabelValues("gateway").Inc()
		}
	}
}
