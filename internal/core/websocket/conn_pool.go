// Package websocket 提供了连接池功能用于管理WebSocket连接
package websocket

import (
	"sync"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
)

// ConnPool 管理WebSocket连接池
type ConnPool struct {
	sync.RWMutex
	conns  map[uint64]*websocket.Conn
	config *config.Config
}

// NewConnPool 创建一个新的连接池
func NewConnPool(cfg *config.Config) *ConnPool {
	return &ConnPool{
		conns:  make(map[uint64]*websocket.Conn),
		config: cfg,
	}
}

// Add 添加一个新连接到池中
func (p *ConnPool) Add(userID uint64, conn *websocket.Conn) {
	p.Lock()
	defer p.Unlock()
	p.conns[userID] = conn
}

// Remove 从池中移除一个连接
func (p *ConnPool) Remove(userID uint64) {
	p.Lock()
	defer p.Unlock()
	delete(p.conns, userID)
}

// UpdateUserID 更新连接的用户ID
func (p *ConnPool) UpdateUserID(userID uint64, conn *websocket.Conn) {
	p.Lock()
	defer p.Unlock()
	p.conns[userID] = conn
}
