package connection

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.uber.org/zap"
)

// Client 表示 WebSocket 客户端
type Client struct {
	cfg      *config.Config
	conn     *websocket.Conn
	userID   uint64
	liveID   uint64
	mutex    sync.RWMutex // 保护连接状态
	isClosed bool
}

// NewClient 创建新的 WebSocket 客户端
func NewClient(cfg *config.Config) (*Client, error) {
	// 建立 WebSocket 连接
	conn, _, err := websocket.Dial(context.Background(), cfg.WebSocket.Endpoint, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"User-Agent": []string{"edulive-client/1.0"},
		},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to dial WebSocket", zap.Error(err))
		return nil, fmt.Errorf("websocket dial failed: %w", err)
	}

	client := &Client{
		cfg:      cfg,
		conn:     conn,
		userID:   cfg.Client.UserID,
		liveID:   cfg.Client.LiveID,
		isClosed: false,
	}

	logger.Info("WebSocket client initialized",
		zap.String("endpoint", cfg.WebSocket.Endpoint),
		zap.Uint64("userID", client.userID),
		zap.Uint64("liveID", client.liveID))

	return client, nil
}

// StartHeartbeat 启动心跳机制
func (c *Client) StartHeartbeat(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second) // 每 30 秒发送一次心跳
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Heartbeat stopped due to context cancellation")
			return nil
		case <-ticker.C:
			if err := c.sendHeartbeat(); err != nil {
				logger.Warn("Failed to send heartbeat", zap.Error(err))
				return fmt.Errorf("heartbeat failed: %w", err)
			}
		}
	}
}

// sendHeartbeat 发送心跳消息
func (c *Client) sendHeartbeat() error {
	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewHeartbeatMessage(c.userID)
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode heartbeat failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		return fmt.Errorf("write heartbeat failed: %w", err)
	}

	logger.Debug("Heartbeat sent", zap.Uint64("userID", c.userID))
	return nil
}

// SendBullet 发送弹幕消息
func (c *Client) SendBullet(content string) error {
	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewBulletMessage(c.liveID, c.userID, content)
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode bullet failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		return fmt.Errorf("write bullet failed: %w", err)
	}

	logger.Info("Bullet sent",
		zap.Uint64("userID", c.userID),
		zap.Uint64("liveID", c.liveID),
		zap.String("content", content))
	return nil
}

// CreateRoom 创建直播间
func (c *Client) CreateRoom(liveID, userID uint64) error {
	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewCreateRoomMessage(liveID, userID)
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode create room message failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 发送创建房间请求
	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		return fmt.Errorf("write create room message failed: %w", err)
	}

	// 读取响应
	_, respData, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read create room response failed: %w", err)
	}

	resp, err := protocol.Decode(respData)
	if err != nil {
		return fmt.Errorf("decode create room response failed: %w", err)
	}

	if resp.Type != protocol.TypeCreateRoom {
		return fmt.Errorf("unexpected response type: %d", resp.Type)
	}

	if resp.Content != "" && resp.Content != "Live room created" {
		return fmt.Errorf("create room failed: %s", resp.Content)
	}

	logger.Info("Create room request sent",
		zap.Uint64("liveID", liveID),
		zap.Uint64("userID", userID))
	return nil
}

// CheckRoom 检查直播间是否存在
func (c *Client) CheckRoom(liveID, userID uint64) error {
	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewCheckRoomMessage(liveID, userID)
	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode check room message failed: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 发送检查房间请求
	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		return fmt.Errorf("write check room message failed: %w", err)
	}

	// 读取响应
	_, respData, err := conn.Read(ctx)
	if err != nil {
		return fmt.Errorf("read check room response failed: %w", err)
	}

	resp, err := protocol.Decode(respData)
	if err != nil {
		return fmt.Errorf("decode check room response failed: %w", err)
	}

	if resp.Type != protocol.TypeCheckRoom {
		return fmt.Errorf("unexpected response type: %d", resp.Type)
	}

	if resp.Content != "exists" {
		return fmt.Errorf("room does not exist: %s", resp.Content)
	}

	logger.Info("Check room request sent",
		zap.Uint64("liveID", liveID),
		zap.Uint64("userID", userID),
		zap.String("result", resp.Content))
	return nil
}

// Close 关闭 WebSocket 连接
func (c *Client) Close() {
	c.mutex.Lock()
	if c.isClosed {
		c.mutex.Unlock()
		return
	}
	c.isClosed = true
	conn := c.conn
	c.mutex.Unlock()

	if conn != nil {
		conn.Close(websocket.StatusNormalClosure, "client closed")
		logger.Info("WebSocket connection closed", zap.Uint64("userID", c.userID))
	}
}
