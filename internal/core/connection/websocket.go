// client/connection/websocket.go
package connection

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability" // 引入可观测性包
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.opentelemetry.io/otel/attribute" // 用于添加 Span 属性
	"go.opentelemetry.io/otel/trace"     // 用于 Span 操作
	"go.uber.org/zap"
)

// Client 表示 WebSocket 客户端
type Client struct {
	config   *config.Config
	conn     *websocket.Conn
	liveID   uint64
	userID   uint64
	userName string
	mutex    sync.RWMutex // 保护连接状态
	isClosed bool
}

// NewClient 创建新的 WebSocket 客户端
func NewClient(cfg *config.Config) (*Client, error) {
	// 创建根 Span 追踪 WebSocket 客户端初始化
	ctx, span := observability.StartSpan(context.Background(), "websocket.NewClient",
		trace.WithAttributes(
			attribute.String("endpoint", cfg.WebSocket.Endpoint),
			attribute.Int64("user_id", int64(cfg.Client.UserID)),
			attribute.Int64("live_id", int64(cfg.Client.LiveID)),
		))
	defer span.End()

	// 建立 WebSocket 连接
	startTime := time.Now()
	conn, _, err := websocket.Dial(ctx, cfg.WebSocket.Endpoint, &websocket.DialOptions{
		HTTPHeader: http.Header{
			"User-Agent": []string{"edulive-client/1.0"},
		},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Error("Failed to dial WebSocket", zap.Error(err))
		observability.RecordError(span, err, "websocket.NewClient")
		return nil, fmt.Errorf("websocket dial failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Dial", time.Since(startTime))

	client := &Client{
		config:   cfg,
		conn:     conn,
		liveID:   cfg.Client.LiveID,
		userID:   cfg.Client.UserID,
		userName: cfg.Client.UserName,
		mutex:    sync.RWMutex{},
		isClosed: false,
	}

	logger.Info("WebSocket client initialized",
		zap.String("endpoint", cfg.WebSocket.Endpoint),
		zap.Uint64("liveID", client.liveID),
		zap.Uint64("userID", client.userID),
		zap.String("userName", client.userName))
	return client, nil
}

// StartHeartbeat 启动心跳机制
func (c *Client) StartHeartbeat(ctx context.Context) error {
	// 创建根 Span 追踪心跳机制
	ctx, span := observability.StartSpan(ctx, "websocket.StartHeartbeat",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
		))
	defer span.End()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("Heartbeat stopped due to context cancellation")
			span.AddEvent("context_canceled")
			return nil
		case <-ticker.C:
			// 创建子 Span 追踪单次心跳
			hbCtx, hbSpan := observability.StartSpan(ctx, "websocket.sendHeartbeat")
			startTime := time.Now()

			if err := c.sendHeartbeat(); err != nil {
				logger.Warn("Failed to send heartbeat", zap.Error(err))
				observability.RecordError(hbSpan, err, "websocket.sendHeartbeat")
				hbSpan.End()
				return fmt.Errorf("heartbeat failed: %w", err)
			}
			observability.RecordLatency(hbCtx, "websocket.sendHeartbeat", time.Since(startTime))
			hbSpan.End()
		}
	}
}

// sendHeartbeat 发送心跳消息
func (c *Client) sendHeartbeat() error {
	// 创建 Span 追踪心跳消息发送
	ctx, span := observability.StartSpan(context.Background(), "websocket.sendHeartbeat",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.String("user_name", c.userName),
		))
	defer span.End()

	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		err := fmt.Errorf("connection is closed")
		observability.RecordError(span, err, "websocket.sendHeartbeat")
		return err
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewHeartbeatMessage(c.userID)
	startTime := time.Now()
	data, err := msg.Encode(c.config.Performance.BulletCompression)
	if err != nil {
		observability.RecordError(span, err, "websocket.sendHeartbeat")
		return fmt.Errorf("encode heartbeat failed: %w", err)
	}
	observability.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	startTime = time.Now()
	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		observability.RecordError(span, err, "websocket.sendHeartbeat")
		return fmt.Errorf("write heartbeat failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	logger.Debug("Heartbeat sent",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName))
	return nil
}

// SendBullet 发送弹幕消息
func (c *Client) SendBullet(content string) error {
	// 创建 Span 追踪弹幕发送
	ctx, span := observability.StartSpan(context.Background(), "websocket.SendBullet",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
			attribute.String("user_name", c.userName),
			attribute.String("content", content),
		))
	defer span.End()

	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		err := fmt.Errorf("connection is closed")
		observability.RecordError(span, err, "websocket.SendBullet")
		return err
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewBulletMessage(c.liveID, c.userID, c.userName, content, "green")
	startTime := time.Now()
	data, err := msg.Encode(c.config.Performance.BulletCompression)
	if err != nil {
		observability.RecordError(span, err, "websocket.SendBullet")
		return fmt.Errorf("encode bullet failed: %w", err)
	}
	observability.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	startTime = time.Now()
	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		observability.RecordError(span, err, "websocket.SendBullet")
		return fmt.Errorf("write bullet failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	logger.Debug("Bullet sent",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName),
		zap.Uint64("liveID", c.liveID),
		zap.String("content", content))
	return nil
}

// CreateRoom 创建直播间
func (c *Client) CreateRoom(liveID, userID uint64, userName string) error {
	// 创建 Span 追踪房间创建
	ctx, span := observability.StartSpan(context.Background(), "websocket.CreateRoom",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
			attribute.Int64("live_id", int64(liveID)),
			attribute.String("user_name", userName),
		))
	defer span.End()

	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		err := fmt.Errorf("connection is closed")
		observability.RecordError(span, err, "websocket.CreateRoom")
		return err
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewCreateRoomMessage(liveID, userID)
	startTime := time.Now()
	data, err := msg.Encode(c.config.Performance.BulletCompression)
	if err != nil {
		observability.RecordError(span, err, "websocket.CreateRoom")
		return fmt.Errorf("encode create room message failed: %w", err)
	}
	observability.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	startTime = time.Now()
	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		observability.RecordError(span, err, "websocket.CreateRoom")
		return fmt.Errorf("write create room message failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	startTime = time.Now()
	_, respData, err := conn.Read(ctx)
	if err != nil {
		observability.RecordError(span, err, "websocket.CreateRoom")
		return fmt.Errorf("read create room response failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Read", time.Since(startTime))

	resp, err := protocol.Decode(respData)
	if err != nil {
		observability.RecordError(span, err, "websocket.CreateRoom")
		return fmt.Errorf("decode create room response failed: %w", err)
	}
	defer resp.Release()

	if resp.Type != protocol.TypeCreateRoom {
		err := fmt.Errorf("unexpected response type: %d", resp.Type)
		observability.RecordError(span, err, "websocket.CreateRoom")
		return err
	}

	if resp.Content != "" && resp.Content != "Live room created" {
		err := fmt.Errorf("create room failed: %s", resp.Content)
		observability.RecordError(span, err, "websocket.CreateRoom")
		return err
	}

	logger.Info("Create room request sent",
		zap.Uint64("liveID", liveID),
		zap.Uint64("userID", userID),
		zap.String("userName", userName))
	return nil
}

// CheckRoom 检查直播间是否存在
func (c *Client) CheckRoom(liveID, userID uint64, userName string) error {
	// 创建 Span 追踪房间检查
	ctx, span := observability.StartSpan(context.Background(), "websocket.CheckRoom",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
			attribute.Int64("live_id", int64(liveID)),
			attribute.String("user_name", userName),
		))
	defer span.End()

	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		err := fmt.Errorf("connection is closed")
		observability.RecordError(span, err, "websocket.CheckRoom")
		return err
	}
	conn := c.conn
	c.mutex.RUnlock()

	msg := protocol.NewCheckRoomMessage(liveID, userID)
	startTime := time.Now()
	data, err := msg.Encode(c.config.Performance.BulletCompression)
	if err != nil {
		observability.RecordError(span, err, "websocket.CheckRoom")
		return fmt.Errorf("encode check room message failed: %w", err)
	}
	observability.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	startTime = time.Now()
	err = conn.Write(ctx, websocket.MessageBinary, data)
	if err != nil {
		observability.RecordError(span, err, "websocket.CheckRoom")
		return fmt.Errorf("write check room message failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	startTime = time.Now()
	_, respData, err := conn.Read(ctx)
	if err != nil {
		observability.RecordError(span, err, "websocket.CheckRoom")
		return fmt.Errorf("read check room response failed: %w", err)
	}
	observability.RecordLatency(ctx, "websocket.Read", time.Since(startTime))

	resp, err := protocol.Decode(respData)
	if err != nil {
		observability.RecordError(span, err, "websocket.CheckRoom")
		return fmt.Errorf("decode check room response failed: %w", err)
	}
	defer resp.Release()

	if resp.Type != protocol.TypeCheckRoom {
		err := fmt.Errorf("unexpected response type: %d", resp.Type)
		observability.RecordError(span, err, "websocket.CheckRoom")
		return err
	}

	if resp.Content != "exists" {
		err := fmt.Errorf("room does not exist: %s", resp.Content)
		observability.RecordError(span, err, "websocket.CheckRoom")
		return err
	}

	logger.Info("Check room request sent",
		zap.Uint64("liveID", liveID),
		zap.Uint64("userID", userID),
		zap.String("userName", userName),
		zap.String("result", resp.Content))
	return nil
}

// Close 关闭 WebSocket 连接
func (c *Client) Close() {
	// 创建 Span 追踪连接关闭
	ctx, span := observability.StartSpan(context.Background(), "websocket.Close",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
		))
	defer span.End()

	c.mutex.Lock()
	if c.isClosed {
		c.mutex.Unlock()
		span.AddEvent("already_closed")
		return
	}
	c.isClosed = true
	conn := c.conn
	c.mutex.Unlock()

	if conn != nil {
		startTime := time.Now()
		conn.Close(websocket.StatusNormalClosure, "client closed")
		observability.RecordLatency(ctx, "websocket.Close", time.Since(startTime))
		logger.Info("WebSocket connection closed",
			zap.Uint64("userID", c.userID),
			zap.String("userName", c.userName))
	}
}
