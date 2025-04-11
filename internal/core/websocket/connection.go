package websocket

import (
	"sync"

	"github.com/coder/websocket"
)

// ConnectionContext 存储与连接相关的上下文信息
type ConnectionContext struct {
	UserID uint64
	LiveID uint64
}

// NewConnectionContext 创建新的连接上下文
func NewConnectionContext() *ConnectionContext {
	return &ConnectionContext{
		UserID: 0,
		LiveID: 0,
	}
}

// ConnectionManager 管理WebSocket连接
type ConnectionManager struct {
	mu    sync.RWMutex
	conns map[uint64]*websocket.Conn
}

// NewConnectionManager 创建新的连接管理器
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		conns: make(map[uint64]*websocket.Conn),
	}
}

// Add 添加连接到管理器
func (m *ConnectionManager) Add(userID uint64, conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[userID] = conn
}

// Remove 从管理器移除连接
func (m *ConnectionManager) Remove(userID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.conns, userID)
}

// UpdateUserID 更新用户ID与连接的映射
func (m *ConnectionManager) UpdateUserID(userID uint64, conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[userID] = conn
}

// GetConnection 获取用户的连接
func (m *ConnectionManager) GetConnection(userID uint64) (*websocket.Conn, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conn, exists := m.conns[userID]
	return conn, exists
}
