package dispatcher

import (
	"context"
	"sync"

	"time"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/quic-go/quic-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// Client 表示连接到分发器的客户端
type Client struct {
	UserID   uint64
	LiveID   uint64
	UserName string
	Stream   quic.Stream
}

// ClientManager 管理所有连接的客户端
type ClientManager struct {
	mutex   sync.RWMutex
	clients map[uint64]*Client
}

// NewClientManager 创建一个新的客户端管理器
func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[uint64]*Client),
	}
}

// AddClient 添加一个客户端
func (m *ClientManager) AddClient(client *Client) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.clients[client.UserID] = client
}

// RemoveClient 移除一个客户端
func (m *ClientManager) RemoveClient(ctx context.Context, userID uint64) {
	ctx, span := ob.StartSpan(ctx, "ClientManager.RemoveClient",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
		))
	defer span.End()

	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 检查客户端是否存在
	client, exists := m.clients[userID]
	if !exists {
		return
	}

	// 关闭客户端流
	startTime := time.Now()
	if err := client.Stream.Close(); err != nil {
		logger.Error("Failed to close client stream",
			zap.Uint64("userID", userID),
			zap.Error(err))
		ob.RecordError(span, err, "ClientManager.RemoveClient")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "close").Inc()
	}

	// 删除客户端
	delete(m.clients, userID)
	ob.RecordLatency(ctx, "quic.Stream.Close", time.Since(startTime))
}

// GetClient 获取特定的客户端
func (m *ClientManager) GetClient(userID uint64) (*Client, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	client, exists := m.clients[userID]
	return client, exists
}

// GetClientsByLiveID 获取特定直播间的所有客户端
func (m *ClientManager) GetClientsByLiveID(liveID uint64) []*Client {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var result []*Client
	for _, client := range m.clients {
		if client.LiveID == liveID {
			result = append(result, client)
		}
	}
	return result
}

// GetAllClients 获取所有客户端的列表
func (m *ClientManager) GetAllClients() []*Client {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	result := make([]*Client, 0, len(m.clients))
	for _, client := range m.clients {
		result = append(result, client)
	}
	return result
}

// ClientCount 获取当前客户端数量
func (m *ClientManager) ClientCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return len(m.clients)
}

// LiveRoomCount 获取当前活跃直播间数量
func (m *ClientManager) LiveRoomCount() int {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	roomSet := make(map[uint64]struct{})
	for _, client := range m.clients {
		roomSet[client.LiveID] = struct{}{}
	}
	return len(roomSet)
}
