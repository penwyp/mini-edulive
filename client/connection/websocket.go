package connection

import (
	"context"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.uber.org/zap"
)

// Client WebSocket 客户端
type Client struct {
	conn   *websocket.Conn
	config *config.Config
	userID uint64
}

// NewClient 创建 WebSocket 客户端
func NewClient(cfg *config.Config) (*Client, error) {
	conn, _, err := websocket.Dial(context.Background(), cfg.WebSocket.Endpoint, nil)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		config: cfg,
		userID: cfg.Client.UserID,
	}, nil
}

// SendBullet 发送弹幕消息
func (c *Client) SendBullet(content string) error {
	msg := protocol.NewBulletMessage(c.userID, content)
	data, err := msg.Encode()
	if err != nil {
		return err
	}

	err = c.conn.Write(context.Background(), websocket.MessageBinary, data)
	if err != nil {
		logger.Warn("Failed to send bullet", zap.Error(err))
		return err
	}
	logger.Info("Bullet sent", zap.Uint64("userID", c.userID), zap.String("content", content))
	return nil
}

// SendHeartbeat 发送心跳消息
func (c *Client) SendHeartbeat() error {
	msg := protocol.NewHeartbeatMessage(c.userID)
	data, err := msg.Encode()
	if err != nil {
		return err
	}

	err = c.conn.Write(context.Background(), websocket.MessageBinary, data)
	if err != nil {
		logger.Warn("Failed to send heartbeat", zap.Error(err))
		return err
	}
	return nil
}

// StartHeartbeat 启动心跳循环
func (c *Client) StartHeartbeat() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := c.SendHeartbeat(); err != nil {
			logger.Error("Heartbeat failed", zap.Error(err))
			return
		}
	}
}

// Receive 接收服务端分发的消息
func (c *Client) Receive(ctx context.Context) {
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			logger.Warn("Failed to receive message", zap.Error(err))
			return
		}

		msg, err := protocol.Decode(data)
		if err != nil {
			logger.Warn("Failed to decode received message", zap.Error(err))
			continue
		}

		logger.Info("Received message", zap.Uint64("userID", msg.UserID), zap.String("content", msg.Content))
		// TODO: 显示弹幕到界面
	}
}

// Close 关闭连接
func (c *Client) Close() {
	c.conn.Close(websocket.StatusNormalClosure, "")
}
