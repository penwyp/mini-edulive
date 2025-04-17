// Package websocket 提供了WebSocket消息处理功能
package websocket

import (
	"context"
	"time"

	"github.com/coder/websocket"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// processMessage 处理接收到的消息
func (s *Server) processMessage(conn *websocket.Conn, data []byte, userID, liveID *uint64) {
	msgCtx, msgSpan := ob.StartSpan(context.Background(), "gateway.processMessage",
		trace.WithAttributes(
			attribute.String("message_type", "unknown"),
			attribute.Int64("user_id", int64(*userID)),
			attribute.Int64("live_id", int64(*liveID)),
		))
	defer msgSpan.End()

	startTime := time.Now()

	// 解码消息
	msg, err := protocol.Decode(data)
	if err != nil {
		logger.Warn("Failed to decode message payload", zap.Error(err))
		ob.RecordError(msgSpan, err, "gateway.processMessage")
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return
	}
	defer msg.Release()

	// 更新用户ID
	if *userID == 0 {
		*userID = msg.UserID
		s.pool.UpdateUserID(*userID, conn)
		msgSpan.SetAttributes(attribute.Int64("user_id", int64(*userID)))
	}

	msgSpan.SetAttributes(
		attribute.String("message_type", messageTypeToString(msg.Type)),
		attribute.Int64("live_id", int64(msg.LiveID)),
	)

	// 根据消息类型处理
	switch msg.Type {
	case protocol.TypeBullet:
		s.handleBulletMessage(msgCtx, msgSpan, msg)
	case protocol.TypeHeartbeat:
		s.handleHeartbeatMessage(msgCtx, msg)
	case protocol.TypeCreateRoom:
		s.handleCreateRoomMessage(msgCtx, msgSpan, conn, msg, liveID)
	case protocol.TypeCheckRoom:
		s.handleCheckRoomMessage(msgCtx, msgSpan, conn, msg)
	}

	ob.RecordLatency(msgCtx, "gateway.processMessage", time.Since(startTime))
	ob.BulletMessageLatency.WithLabelValues("gateway", messageTypeToString(msg.Type)).Observe(time.Since(startTime).Seconds())
}

// handleBulletMessage 处理弹幕消息
func (s *Server) handleBulletMessage(ctx context.Context, parentSpan trace.Span, msg *protocol.BulletMessage) {
	// 创建新的 span，继承父上下文
	bulletCtx, bulletSpan := ob.StartSpan(ctx, "gateway.handleBulletMessage",
		trace.WithAttributes(
			attribute.String("message_type", messageTypeToString(msg.Type)),
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer bulletSpan.End()

	startTime := time.Now()

	logger.Info("Received bullet message",
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName),
		zap.Uint64("liveID", msg.LiveID),
		zap.String("content", msg.Content))

	err := s.sendToKafka(bulletCtx, msg)
	if err != nil {
		logger.Error("Failed to forward bullet message to Kafka", zap.Error(err))
		ob.RecordError(bulletSpan, err, "gateway.handleBulletMessage")
		ob.BulletMessagesProcessed.WithLabelValues("gateway", "failed").Inc()
	} else {
		logger.Info("Bullet message successfully forwarded to Kafka",
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName),
			zap.Uint64("liveID", msg.LiveID))
		ob.BulletMessagesProcessed.WithLabelValues("gateway", "success").Inc()
	}

	// 记录延迟
	ob.RecordLatency(bulletCtx, "gateway.handleBulletMessage", time.Since(startTime))
}

// handleHeartbeatMessage 处理心跳消息
func (s *Server) handleHeartbeatMessage(ctx context.Context, msg *protocol.BulletMessage) {
	_, heartbeatSpan := ob.StartSpan(ctx, "gateway.handleHeartbeatMessage",
		trace.WithAttributes(
			attribute.String("message_type", messageTypeToString(msg.Type)),
			attribute.Int64("user_id", int64(msg.UserID)),
		))
	defer heartbeatSpan.End()

	logger.Info("Received heartbeat message",
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))
	ob.HeartbeatFailures.WithLabelValues("gateway").Inc()
}

// handleCreateRoomMessage 处理创建直播间消息
func (s *Server) handleCreateRoomMessage(spanCtx context.Context, span trace.Span,
	conn *websocket.Conn, msg *protocol.BulletMessage, liveID *uint64) {
	// 创建新的 span，继承父上下文
	roomCtx, bulletSpan := ob.StartSpan(spanCtx, "gateway.handleCreateRoomMessage",
		trace.WithAttributes(
			attribute.String("message_type", messageTypeToString(msg.Type)),
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer bulletSpan.End()

	logger.Info("Received create room request",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))

	// 检查直播间是否已存在
	if exist, isBackdoor := s.isLiveRoomExists(roomCtx, msg.LiveID); exist && !isBackdoor {
		s.handleDuplicateLiveRoom(roomCtx, span, conn, msg)
		return
	}

	// 注册新直播间
	err := s.registerLiveRoom(roomCtx, msg.LiveID)
	if err != nil {
		logger.Error("Failed to register live room", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleCreateRoomMessage")
		ob.LiveRoomCreations.WithLabelValues("failed").Inc()
		return
	}

	ob.LiveRoomCreations.WithLabelValues("success").Inc()
	*liveID = msg.LiveID

	// 发送成功响应
	s.sendCreateRoomResponse(roomCtx, span, conn, msg)
}

// handleDuplicateLiveRoom 处理重复的直播间创建请求
func (s *Server) handleDuplicateLiveRoom(spanCtx context.Context, span trace.Span,
	conn *websocket.Conn, msg *protocol.BulletMessage) {
	// 创建新的 span，继承父上下文
	roomCtx, bulletSpan := ob.StartSpan(spanCtx, "gateway.handleCreateRoomMessage",
		trace.WithAttributes(
			attribute.String("message_type", messageTypeToString(msg.Type)),
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer bulletSpan.End()

	ob.LiveRoomCreations.WithLabelValues("failed").Inc()

	resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID, msg.UserName)
	resp.Content = "Live room already exists"

	respData, err := resp.Encode(s.config.Performance.BulletCompression)
	if err != nil {
		logger.Error("Failed to encode error response", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleDuplicateLiveRoom")
		ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Write(roomCtx, websocket.MessageBinary, respData)
	if err != nil {
		logger.Error("Failed to send error response", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleDuplicateLiveRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Close(websocket.StatusInvalidFramePayloadData, "Duplicate liveID")
	if err != nil {
		logger.Error("Failed to close WebSocket connection", zap.Error(err))
		ob.RecordError(span, err, "gateway.handleDuplicateLiveRoom")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "close").Inc()
	}

	resp.Release()
}

// sendCreateRoomResponse 发送创建直播间成功的响应
func (s *Server) sendCreateRoomResponse(ctx context.Context, span trace.Span,
	conn *websocket.Conn, msg *protocol.BulletMessage) {

	resp := protocol.NewCreateRoomMessage(msg.LiveID, msg.UserID, msg.UserName)
	respData, err := resp.Encode(s.config.Performance.BulletCompression)
	if err != nil {
		logger.Error("Failed to encode room creation response", zap.Error(err))
		ob.RecordError(span, err, "gateway.sendCreateRoomResponse")
		ob.ProtocolParseErrors.WithLabelValues("create_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Write(ctx, websocket.MessageBinary, respData)
	if err != nil {
		logger.Error("Failed to send room creation response", zap.Error(err))
		ob.RecordError(span, err, "gateway.sendCreateRoomResponse")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	logger.Info("Create room response sent successfully",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))
	resp.Release()
}

// handleCheckRoomMessage 处理检查直播间是否存在的消息
func (s *Server) handleCheckRoomMessage(ctx context.Context, span trace.Span,
	conn *websocket.Conn, msg *protocol.BulletMessage) {

	logger.Info("Received check room request",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName))

	exists, _ := s.isLiveRoomExists(ctx, msg.LiveID)
	content := "exists"
	if !exists {
		content = "not_exists"
	}

	ob.LiveRoomChecks.WithLabelValues(content).Inc()
	s.sendCheckRoomResponse(ctx, span, conn, msg, content)
}

// sendCheckRoomResponse 发送检查直播间存在性的响应
func (s *Server) sendCheckRoomResponse(ctx context.Context, span trace.Span,
	conn *websocket.Conn, msg *protocol.BulletMessage, content string) {

	resp := protocol.NewCheckRoomMessage(msg.LiveID, msg.UserID)
	resp.Content = content

	respData, err := resp.Encode(s.config.Performance.BulletCompression)
	if err != nil {
		logger.Error("Failed to encode check room response", zap.Error(err))
		ob.RecordError(span, err, "gateway.sendCheckRoomResponse")
		ob.ProtocolParseErrors.WithLabelValues("check_room").Inc()
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	err = conn.Write(ctx, websocket.MessageBinary, respData)
	if err != nil {
		logger.Error("Failed to send check room response", zap.Error(err))
		ob.RecordError(span, err, "gateway.sendCheckRoomResponse")
		ob.WebSocketConnectionErrors.WithLabelValues("gateway", "write").Inc()
		resp.Release()
		return
	}

	logger.Info("Check room response sent successfully",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("result", content))
	resp.Release()
}

// messageTypeToString 将消息类型转换为字符串，便于追踪
func messageTypeToString(t uint8) string {
	switch t {
	case protocol.TypeBullet:
		return "bullet"
	case protocol.TypeHeartbeat:
		return "heartbeat"
	case protocol.TypeCreateRoom:
		return "create_room"
	case protocol.TypeCheckRoom:
		return "check_room"
	default:
		return "unknown"
	}
}
