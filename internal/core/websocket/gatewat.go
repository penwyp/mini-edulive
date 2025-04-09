package websocket

import (
	"context"
	"encoding/binary"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Server WebSocket 服务端
type Server struct {
	pool   *ConnPool
	config *config.Config
	kafka  *kafka.Writer // Kafka 生产者
}

// NewServer 创建 WebSocket 服务端
func NewServer(cfg *config.Config) *Server {
	// 初始化 Kafka Writer
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Kafka.Brokers...), // 使用配置中的 Kafka Brokers
		Topic:    cfg.Kafka.Topic,                 // 使用配置中的 Kafka Topic
		Balancer: getBalancer(cfg.Kafka.Balancer), // 使用 Hash 分区策略，确保同一 LiveID 分配到同一分区
	}

	return &Server{
		pool:   NewConnPool(cfg),
		config: cfg,
		kafka:  kafkaWriter,
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
		CompressionMode: websocket.CompressionDisabled, // 二进制协议无需压缩
	})
	if err != nil {
		logger.Error("Failed to accept WebSocket connection", zap.Error(err))
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// 添加连接到池中
	userID := uint64(0) // 初始假设用户ID未知
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

		// 更新用户ID（首次消息可能携带）
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

			// 将消息转发到 Kafka，按照 LiveID 分区
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
			// TODO: 更新连接活跃状态
		}
	}
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
