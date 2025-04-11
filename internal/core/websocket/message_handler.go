package websocket

import (
	"context"
	"time"

	"github.com/penwyp/mini-edulive/config"

	"github.com/coder/websocket"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// BackDoorLiveRoomID 是一个特殊的直播间ID，用于测试或调试
const BackDoorLiveRoomID = 10000

// MessageHandler 负责处理WebSocket消息
type MessageHandler struct {
	processor *MessageProcessor
	config    *config.Config
}

// NewMessageHandler 创建新的消息处理程序
func NewMessageHandler(processor *MessageProcessor, cfg *config.Config) *MessageHandler {
	return &MessageHandler{
		processor: processor,
		config:    cfg,
	}
}

// HandleMessage 处理接收到的消息
func (h *MessageHandler) HandleMessage(ctx context.Context, conn *websocket.Conn, data []byte, connCtx *ConnectionContext, workerID int) {
	msgCtx, msgSpan := ob.StartSpan(ctx, "gateway.processMessage",
		trace.WithAttributes(
			attribute.String("message_type", "unknown"),
			attribute.Int64("user_id", int64(connCtx.UserID)),
			attribute.Int64("live_id", int64(connCtx.LiveID)),
			attribute.Int("worker_id", workerID),
		))
	defer msgSpan.End()

	startTime := time.Now()

	msg, err := protocol.Decode(data)
	if err != nil {
		logger.Warn("Failed to decode message", zap.Error(err))
		ob.RecordError(msgSpan, err, "gateway.processMessage")
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return
	}
	defer msg.Release()

	// 更新用户ID（如果还没设置）
	if connCtx.UserID == 0 {
		connCtx.UserID = msg.UserID
		msgSpan.SetAttributes(attribute.Int64("user_id", int64(connCtx.UserID)))
	}

	msgSpan.SetAttributes(
		attribute.String("message_type", messageTypeToString(msg.Type)),
		attribute.Int64("live_id", int64(msg.LiveID)),
	)

	// 根据消息类型处理
	switch msg.Type {
	case protocol.TypeBullet:
		h.handleBulletMessage(msgCtx, conn, msg)

	case protocol.TypeHeartbeat:
		h.handleHeartbeatMessage(msgCtx, msg)

	case protocol.TypeCreateRoom:
		h.handleCreateRoomMessage(msgCtx, conn, msg, connCtx)

	case protocol.TypeCheckRoom:
		h.handleCheckRoomMessage(msgCtx, conn, msg)
	}

	ob.RecordLatency(msgCtx, "gateway.processMessage", time.Since(startTime))
	ob.BulletMessageLatency.WithLabelValues("gateway", messageTypeToString(msg.Type)).Observe(time.Since(startTime).Seconds())
}

// handleBulletMessage 处理弹幕消息
func (h *MessageHandler) handleBulletMessage(ctx context.Context, conn *websocket.Conn, msg *protocol.BulletMessage) {
	logger.Info("Received bullet message",
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName),
		zap.Uint64("liveID", msg.LiveID),
		zap.String("content", msg.Content))

	err := h.processor.SendToKafka(ctx, msg)
	if err != nil {
		logger.Error("Failed to send message to Kafka", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.BulletMessagesProcessed.WithLabelValues("gateway", "failed").Inc()
	} else {
		logger.Info("Message sent to Kafka",
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName),
			zap.Uint64("liveID", msg.LiveID),
			zap.String("content", msg.Content))
		ob.BulletMessagesProcessed.WithLabelValues("gateway", "success").Inc()
	}
}

// handleHeartbeatMessage 处理心跳消息
func (h *MessageHandler) handleHeartbeatMessage(ctx context.Context, msg *protocol.BulletMessage) {
	logger.Info("Received heartbeat",
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))
	ob.HeartbeatFailures.WithLabelValues("gateway").Inc()
}

// handleCreateRoomMessage 处理创建房间消息
func (h *MessageHandler) handleCreateRoomMessage(ctx context.Context, conn *websocket.Conn, msg *protocol.BulletMessage, connCtx *ConnectionContext) {
	logger.Info("Received create room request",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))

	// 检查房间是否已存在
	exists, isBackdoor := h.processor.IsRoomExists(ctx, msg.LiveID)
	if exists && !isBackdoor {
		h.sendRoomExistsError(ctx, conn, msg)
		return
	}

	// 注册新房间
	err := h.processor.RegisterRoom(ctx, msg.LiveID)
	if err != nil {
		logger.Error("Failed to register live room", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.LiveRoomCreations.WithLabelValues("failed").Inc()
		return
	}

	ob.LiveRoomCreations.WithLabelValues("success").Inc()
	connCtx.LiveID = msg.LiveID

	// 发送创建成功响应
	h.sendCreateRoomResponse(ctx, conn, msg)
}

// sendRoomExistsError 发送房间已存在的错误
func (h *MessageHandler) sendRoomExistsError(ctx context.Context, conn *websocket.Conn, msg *protocol.BulletMessage) {
	ob.LiveRoomCreations.WithLabelValues("failed").Inc()

	resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID, msg.UserName)
	resp.Content = "Live room already exists"

	respData, err := resp.Encode(h.config.GetBulletCompression())
	if err != nil {
		logger.Error("Failed to encode error response", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Write(ctx, websocket.MessageBinary, respData)
	if err != nil {
		logger.Error("Failed to send error response", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Close(websocket.StatusInvalidFramePayloadData, "Duplicate liveID")
	if err != nil {
		logger.Error("Failed to close WebSocket connection", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "close").Inc()
	}
	resp.Release()
}

// sendCreateRoomResponse 发送创建房间成功的响应
func (h *MessageHandler) sendCreateRoomResponse(ctx context.Context, conn *websocket.Conn, msg *protocol.BulletMessage) {
	resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID, msg.UserName)
	respData, err := resp.Encode(h.config.GetBulletCompression())
	if err != nil {
		logger.Error("Failed to encode response", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Write(ctx, websocket.MessageBinary, respData)
	if err != nil {
		logger.Error("Failed to send response", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	logger.Info("Create room response sent",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))
	resp.Release()
}

// handleCheckRoomMessage 处理检查房间消息
func (h *MessageHandler) handleCheckRoomMessage(ctx context.Context, conn *websocket.Conn, msg *protocol.BulletMessage) {
	logger.Info("Received check room request",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))

	exists, _ := h.processor.IsRoomExists(ctx, msg.LiveID)
	content := "exists"
	if !exists {
		content = "not_exists"
	}

	ob.LiveRoomChecks.WithLabelValues(content).Inc()

	resp := protocol.NewCheckRoomMessage(msg.LiveID, msg.UserID)
	resp.Content = content
	respData, err := resp.Encode(h.config.GetBulletCompression())
	if err != nil {
		logger.Error("Failed to encode check room response", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.ProtocolParseErrors.WithLabelValues("check_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Write(ctx, websocket.MessageBinary, respData)
	if err != nil {
		logger.Error("Failed to send check room response", zap.Error(err))
		ob.RecordError(trace.SpanFromContext(ctx), err, "gateway.processMessage")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	logger.Info("Check room response sent",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName),
		zap.String("result", content))
	resp.Release()
}

// RegisterBackdoorRoom 注册后门房间
func (h *MessageHandler) RegisterBackdoorRoom(ctx context.Context) error {
	return h.processor.RegisterRoom(ctx, BackDoorLiveRoomID)
}

// UnregisterRoom 注销房间
func (h *MessageHandler) UnregisterRoom(ctx context.Context, liveID uint64) {
	h.processor.UnregisterRoom(ctx, liveID)
}
