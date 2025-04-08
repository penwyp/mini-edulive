package websocket

import (
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.uber.org/zap"
)

// Server WebSocket 服务端
type Server struct {
	pool   *ConnPool
	config *config.Config
}

// NewServer 创建 WebSocket 服务端
func NewServer(cfg *config.Config) *Server {
	return &Server{
		pool:   NewConnPool(cfg),
		config: cfg,
	}
}

// Start 启动 WebSocket 服务
func (s *Server) Start() {
	http.HandleFunc("/bullet", s.handleWebSocket)
	logger.Info("WebSocket server starting", zap.String("port", s.config.Server.Port))
	if err := http.ListenAndServe(":"+s.config.Server.Port, nil); err != nil {
		logger.Error("WebSocket server failed to start", zap.Error(err))
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
	userID := uint64(0) // 这里假设从请求头或认证中获取用户ID
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
			logger.Info("Received bullet message", zap.Uint64("userID", msg.UserID), zap.String("content", msg.Content))
			// TODO: 将消息发送到 Kafka（交给 processor 处理）
		case protocol.TypeHeartbeat:
			logger.Debug("Received heartbeat", zap.Uint64("userID", msg.UserID))
			// TODO: 更新连接活跃状态
		}
	}
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
