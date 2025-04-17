// Package websocket 提供了WebSocket服务器实现，用于实时直播消息传输
package websocket

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// BackDoorLiveRoomID 是一个特殊的直播间ID，用于测试或调试目的
const BackDoorLiveRoomID = 10000

// Server 代表WebSocket服务器，处理连接和消息传递
type Server struct {
	pool        *ConnPool
	config      *config.Config
	kafka       *kafka.Writer
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewServer 创建一个新的WebSocket服务器实例
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

// Start 启动WebSocket服务器并开始监听指定端口
func (s *Server) Start(spanCtx context.Context) {
	ctx, span := ob.StartSpan(spanCtx, "gateway.Start",
		trace.WithAttributes(
			attribute.String("port", s.config.App.Port),
		))
	defer span.End()

	http.HandleFunc("/bullet", s.handleWebSocket)
	logger.Info("WebSocket gateway starting on port", zap.String("port", s.config.App.Port),
		zap.String("endpoint", "/bullet"))
	if err := http.ListenAndServe(":"+s.config.App.Port, nil); err != nil {
		logger.Error("WebSocket gateway failed to start", zap.Error(err))
		ob.RecordError(span, err, "gateway.Start")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "setup").Inc()
	}
	_ = ctx
}

// handleWebSocket 处理新进入的WebSocket连接
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ctx, span := ob.StartSpan(r.Context(), "gateway.handleWebSocket")
	defer span.End()

	// 接受WebSocket连接
	conn, err := s.acceptWebSocketConnection(ctx, w, r)
	if err != nil {
		ob.RecordError(span, err, "gateway.handleWebSocket")
		return
	}

	userID := uint64(0)
	liveID := uint64(0)
	s.pool.Add(userID, conn)

	// 注册测试用直播间
	if err := s.registerLiveRoom(ctx, BackDoorLiveRoomID); err != nil {
		logger.Error("Failed to register backdoor live room", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleWebSocket")
		return
	}

	// 设置连接关闭时的清理操作
	defer s.cleanupConnection(ctx, conn, userID, liveID)

	// 处理消息循环
	s.messageLoop(ctx, conn, &userID, &liveID)
}

// acceptWebSocketConnection 接受并建立WebSocket连接
func (s *Server) acceptWebSocketConnection(ctx context.Context, w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	startTime := time.Now()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})

	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "accept").Inc()
		return nil, err
	}

	ob.ActiveWebSocketConnections.WithLabelValues("gateway").Inc()
	ob.RecordLatency(ctx, "websocket.Accept", time.Since(startTime))
	logger.Info("Accepted new WebSocket connection")

	return conn, nil
}

// cleanupConnection 清理并关闭WebSocket连接
func (s *Server) cleanupConnection(ctx context.Context, conn *websocket.Conn, userID uint64, liveID uint64) {
	ob.ActiveWebSocketConnections.WithLabelValues("gateway").Dec()

	if liveID != 0 {
		s.unregisterLiveRoom(ctx, liveID)
	}

	startTime := time.Now()
	err := conn.Close(websocket.StatusNormalClosure, "Connection closed")
	if err != nil {
		logger.Error("Failed to close WebSocket connection", zap.Error(err))
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "close").Inc()
	}

	ob.RecordLatency(ctx, "websocket.Close", time.Since(startTime))
	s.pool.Remove(userID)
	logger.Info("WebSocket connection cleaned up and closed", zap.Uint64("userID", userID))
}

// messageLoop 处理WebSocket消息循环
func (s *Server) messageLoop(ctx context.Context, conn *websocket.Conn, userID *uint64, liveID *uint64) {
	// 创建消息通道和工作池
	// Create message channel and worker pool
	msgChan := make(chan []byte, s.config.Gateway.GetMessageBufferSize())
	workerPool := make(chan struct{}, s.config.Gateway.GetWorkerThread())
	var wg sync.WaitGroup

	// 启动工作池
	for i := 0; i < s.config.Gateway.GetWorkerThread(); i++ {
		wg.Add(1)
		go s.startWorker(ctx, conn, msgChan, workerPool, userID, liveID, &wg)
	}

	// 主循环读取消息
	s.readMessages(ctx, conn, msgChan)

	// 关闭消息通道并等待所有工作者完成
	close(msgChan)
	wg.Wait()
}

// startWorker 启动一个工作者协程处理消息
func (s *Server) startWorker(ctx context.Context, conn *websocket.Conn,
	msgChan <-chan []byte, workerPool chan struct{},
	userID *uint64, liveID *uint64, wg *sync.WaitGroup) {

	defer wg.Done()
	for data := range msgChan {
		workerPool <- struct{}{} // 获取工作槽 (acquire worker slot)
		s.processMessage(conn, data, userID, liveID)
		<-workerPool // 释放工作槽 (release worker slot)
	}
}

// readMessages 从WebSocket连接读取消息
func (s *Server) readMessages(ctx context.Context, conn *websocket.Conn, msgChan chan<- []byte) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("WebSocket context canceled, stopping message reader", zap.Error(ctx.Err()))
			return
		default:
			_, data, err := conn.Read(ctx)
			if err != nil {
				if websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
					websocket.CloseStatus(err) == websocket.StatusGoingAway {
					logger.Info("WebSocket connection closed normally by client")
				} else {
					logger.Warn("Error reading from WebSocket connection", zap.Error(err))
					ob.WebSocketConnectionErrors.WithLabelValues("gateway", "read").Inc()
				}
				return
			}

			// 将消息放入通道
			select {
			case msgChan <- data:
				ob.WebSocketMessagesReceived.WithLabelValues("gateway").Inc()
			default:
				logger.Warn("Message channel at capacity, dropping incoming message")
				ob.WebSocketMessagesDropped.WithLabelValues("gateway").Inc()
			}
		}
	}
}
