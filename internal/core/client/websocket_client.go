package client

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// WebSocketClient WebSocket客户端实现
type WebSocketClient struct {
	config   *config.Config
	options  *ClientOptions
	conn     *websocket.Conn
	liveID   uint64
	userID   uint64
	userName string
	mutex    sync.RWMutex // 保护连接状态
	isClosed bool
}

// NewWebSocketClient 创建一个新的WebSocket客户端
func NewWebSocketClient(ctx context.Context, cfg *config.Config) (*WebSocketClient, error) {
	ctx, span := ob.StartSpan(ctx, "websocket.NewClient",
		trace.WithAttributes(
			attribute.String("endpoint", cfg.WebSocket.Endpoint),
			attribute.Int64("user_id", int64(cfg.Client.UserID)),
			attribute.Int64("live_id", int64(cfg.Client.LiveID)),
		))
	defer span.End()

	// 创建默认选项
	options := DefaultClientOptions()
	options.Compression = cfg.Performance.BulletCompression

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
		ob.RecordError(span, err, "websocket.NewClient")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "accept").Inc()
		return nil, NewConnectionError("dial", err)
	}
	ob.ActiveWebSocketConnections.WithLabelValues("client").Inc()
	ob.RecordLatency(ctx, "websocket.Dial", time.Since(startTime))

	client := &WebSocketClient{
		config:   cfg,
		options:  options,
		conn:     conn,
		liveID:   cfg.Client.LiveID,
		userID:   cfg.Client.UserID,
		userName: cfg.Client.UserName,
		isClosed: false,
	}

	logger.Info("WebSocket client initialized",
		zap.String("endpoint", cfg.WebSocket.Endpoint),
		zap.Uint64("liveID", client.liveID),
		zap.Uint64("userID", client.userID),
		zap.String("userName", client.userName))
	return client, nil
}

// Send 发送弹幕消息
func (c *WebSocketClient) Send(ctx context.Context, content string) error {
	return c.SendBullet(ctx, content)
}

// SendBullet 发送弹幕消息
func (c *WebSocketClient) SendBullet(ctx context.Context, content string) error {
	ctx, span := ob.StartSpan(ctx, "websocket.SendBullet",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
			attribute.String("user_name", c.userName),
			attribute.String("content", content),
		))
	defer span.End()

	// 检查连接状态
	if err := c.checkConnection(); err != nil {
		ob.RecordError(span, err, "websocket.SendBullet")
		return err
	}

	// 创建弹幕消息
	msg := protocol.NewBulletMessage(c.liveID, c.userID, c.userName, time.Now().UnixMilli(), content, "green")

	// 编码消息
	startTime := time.Now()
	data, err := msg.Encode(c.options.Compression)
	if err != nil {
		ob.RecordError(span, err, "websocket.SendBullet")
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewProtocolError("encode", err)
	}
	ob.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	// 发送消息
	writeCtx, cancel := context.WithTimeout(ctx, c.options.WriteTimeout)
	defer cancel()

	startTime = time.Now()
	err = c.conn.Write(writeCtx, websocket.MessageBinary, data)
	if err != nil {
		ob.RecordError(span, err, "websocket.SendBullet")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewConnectionError("write", err)
	}
	ob.RecordLatency(ctx, "websocket.Write", time.Since(startTime))
	ob.BulletMessagesProcessed.WithLabelValues("client", "success").Inc()

	logger.Debug("Bullet sent",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName),
		zap.Uint64("liveID", c.liveID),
		zap.String("content", content))
	return nil
}

// CreateRoom 创建直播间
func (c *WebSocketClient) CreateRoom(ctx context.Context, liveID, userID uint64, userName string) error {
	ctx, span := ob.StartSpan(ctx, "websocket.CreateRoom",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
			attribute.Int64("live_id", int64(liveID)),
			attribute.String("user_name", userName),
		))
	defer span.End()

	// 检查连接状态
	if err := c.checkConnection(); err != nil {
		ob.RecordError(span, err, "websocket.CreateRoom")
		return err
	}

	// 创建创建房间消息
	msg := protocol.NewCreateRoomMessage(liveID, userID, userName)

	// 编码消息
	startTime := time.Now()
	data, err := msg.Encode(c.options.Compression)
	if err != nil {
		ob.RecordError(span, err, "websocket.CreateRoom")
		ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewProtocolError("encode", err)
	}
	ob.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	// 发送消息
	writeCtx, cancel := context.WithTimeout(ctx, c.options.WriteTimeout)
	defer cancel()

	startTime = time.Now()
	err = c.conn.Write(writeCtx, websocket.MessageBinary, data)
	if err != nil {
		ob.RecordError(span, err, "websocket.CreateRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewConnectionError("write", err)
	}
	ob.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	// 读取响应
	readCtx, cancel := context.WithTimeout(ctx, c.options.ReadTimeout)
	defer cancel()

	startTime = time.Now()
	_, respData, err := c.conn.Read(readCtx)
	if err != nil {
		ob.RecordError(span, err, "websocket.CreateRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "read").Inc()
		return NewConnectionError("read", err)
	}
	ob.RecordLatency(ctx, "websocket.Read", time.Since(startTime))

	// 解码响应
	resp, err := protocol.Decode(respData)
	if err != nil {
		ob.RecordError(span, err, "websocket.CreateRoom")
		ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
		return NewProtocolError("decode", err)
	}
	defer resp.Release()

	// 验证响应类型
	if resp.Type != protocol.TypeCreateRoom {
		err := fmt.Errorf("%w: %d", ErrUnexpectedResponseType, resp.Type)
		ob.RecordError(span, err, "websocket.CreateRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "read").Inc()
		return err
	}

	// 检查响应内容
	if resp.Content != "" && resp.Content != "Live room created" {
		err := fmt.Errorf("create room failed: %s", resp.Content)
		ob.RecordError(span, err, "websocket.CreateRoom")
		ob.LiveRoomCreations.WithLabelValues("failed").Inc()
		return err
	}
	ob.LiveRoomCreations.WithLabelValues("success").Inc()

	logger.Info("Create room request sent",
		zap.Uint64("liveID", liveID),
		zap.Uint64("userID", userID),
		zap.String("userName", userName))
	return nil
}

// CheckRoom 检查直播间是否存在
func (c *WebSocketClient) CheckRoom(ctx context.Context, liveID, userID uint64, userName string) error {
	ctx, span := ob.StartSpan(ctx, "websocket.CheckRoom",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
			attribute.Int64("live_id", int64(liveID)),
			attribute.String("user_name", userName),
		))
	defer span.End()

	// 检查连接状态
	if err := c.checkConnection(); err != nil {
		ob.RecordError(span, err, "websocket.CheckRoom")
		return err
	}

	// 创建检查房间消息
	msg := protocol.NewCheckRoomMessage(liveID, userID)

	// 编码消息
	startTime := time.Now()
	data, err := msg.Encode(c.options.Compression)
	if err != nil {
		ob.RecordError(span, err, "websocket.CheckRoom")
		ob.ProtocolParseErrors.WithLabelValues("check_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewProtocolError("encode", err)
	}
	ob.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	// 发送消息
	writeCtx, cancel := context.WithTimeout(ctx, c.options.WriteTimeout)
	defer cancel()

	startTime = time.Now()
	err = c.conn.Write(writeCtx, websocket.MessageBinary, data)
	if err != nil {
		ob.RecordError(span, err, "websocket.CheckRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewConnectionError("write", err)
	}
	ob.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	// 读取响应
	readCtx, cancel := context.WithTimeout(ctx, c.options.ReadTimeout)
	defer cancel()

	startTime = time.Now()
	_, respData, err := c.conn.Read(readCtx)
	if err != nil {
		ob.RecordError(span, err, "websocket.CheckRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "read").Inc()
		return NewConnectionError("read", err)
	}
	ob.RecordLatency(ctx, "websocket.Read", time.Since(startTime))

	// 解码响应
	resp, err := protocol.Decode(respData)
	if err != nil {
		ob.RecordError(span, err, "websocket.CheckRoom")
		ob.ProtocolParseErrors.WithLabelValues("check_room").Inc()
		return NewProtocolError("decode", err)
	}
	defer resp.Release()

	// 验证响应类型
	if resp.Type != protocol.TypeCheckRoom {
		err := fmt.Errorf("%w: %d", ErrUnexpectedResponseType, resp.Type)
		ob.RecordError(span, err, "websocket.CheckRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "read").Inc()
		return err
	}

	// 检查响应内容
	if resp.Content != "exists" {
		err := fmt.Errorf("%w: %s", ErrRoomDoesNotExist, resp.Content)
		ob.RecordError(span, err, "websocket.CheckRoom")
		ob.LiveRoomChecks.WithLabelValues("not_exists").Inc()
		return err
	}
	ob.LiveRoomChecks.WithLabelValues("exists").Inc()

	logger.Info("Check room success",
		zap.Uint64("liveID", liveID),
		zap.Uint64("userID", userID),
		zap.String("userName", userName),
		zap.String("result", resp.Content))
	return nil
}

// StartHeartbeat 启动心跳机制
func (c *WebSocketClient) StartHeartbeat(ctx context.Context) error {
	ctx, span := ob.StartSpan(ctx, "websocket.StartHeartbeat",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
		))
	defer span.End()

	ticker := time.NewTicker(c.options.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("Heartbeat stopped due to context cancellation")
			span.AddEvent("context_canceled")
			return nil
		case <-ticker.C:
			// 发送心跳
			hbCtx, hbSpan := ob.StartSpan(ctx, "websocket.sendHeartbeat")
			startTime := time.Now()

			if err := c.sendHeartbeat(hbCtx); err != nil {
				logger.Warn("Failed to send heartbeat", zap.Error(err))
				ob.RecordError(hbSpan, err, "websocket.sendHeartbeat")
				ob.HeartbeatFailures.WithLabelValues("client").Inc()
				ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
				hbSpan.End()
				return err
			}
			ob.RecordLatency(hbCtx, "websocket.sendHeartbeat", time.Since(startTime))
			hbSpan.End()
		}
	}
}

// sendHeartbeat 发送心跳消息
func (c *WebSocketClient) sendHeartbeat(ctx context.Context) error {
	ctx, span := ob.StartSpan(ctx, "websocket.sendHeartbeat",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.String("user_name", c.userName),
		))
	defer span.End()

	// 检查连接状态
	if err := c.checkConnection(); err != nil {
		ob.RecordError(span, err, "websocket.sendHeartbeat")
		return err
	}

	// 创建心跳消息
	msg := protocol.NewHeartbeatMessage(c.userID, c.userName)

	// 编码消息
	startTime := time.Now()
	data, err := msg.Encode(c.options.Compression)
	if err != nil {
		ob.RecordError(span, err, "websocket.sendHeartbeat")
		ob.ProtocolParseErrors.WithLabelValues("heartbeat").Inc()
		return NewProtocolError("encode", err)
	}
	ob.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	// 发送消息
	writeCtx, cancel := context.WithTimeout(ctx, c.options.WriteTimeout)
	defer cancel()

	startTime = time.Now()
	err = c.conn.Write(writeCtx, websocket.MessageBinary, data)
	if err != nil {
		ob.RecordError(span, err, "websocket.sendHeartbeat")
		ob.WebSocketConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewConnectionError("write", err)
	}
	ob.RecordLatency(ctx, "websocket.Write", time.Since(startTime))

	logger.Debug("Heartbeat sent",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName))
	return nil
}

// Close 关闭 WebSocket 连接
func (c *WebSocketClient) Close(ctx context.Context) {
	ctx, span := ob.StartSpan(ctx, "websocket.Close",
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
		err := conn.Close(websocket.StatusNormalClosure, "client closed")
		if err != nil {
			logger.Error("Failed to close WebSocket connection", zap.Error(err))
			ob.RecordError(span, err, "websocket.Close")
			ob.WebSocketConnectionErrors.WithLabelValues("client", "close").Inc()
		}
		ob.RecordLatency(ctx, "websocket.Close", time.Since(startTime))
		ob.ActiveWebSocketConnections.WithLabelValues("client").Dec()
		logger.Info("WebSocket connection closed",
			zap.Uint64("userID", c.userID),
			zap.String("userName", c.userName))
	}
}

// checkConnection 检查连接状态
func (c *WebSocketClient) checkConnection() error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.isClosed {
		return ErrConnectionClosed
	}
	return nil
}
