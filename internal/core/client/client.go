package client

import (
	"context"
	"time"

	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

// Client 定义了客户端的通用接口
type Client interface {
	// Send 发送消息
	Send(ctx context.Context, content string) error

	// CreateRoom 创建直播间
	CreateRoom(ctx context.Context, liveID, userID uint64, userName string) error

	// CheckRoom 检查直播间是否存在
	CheckRoom(ctx context.Context, liveID, userID uint64, userName string) error

	// StartHeartbeat 启动心跳机制
	StartHeartbeat(ctx context.Context) error

	// Close 关闭连接
	Close(ctx context.Context)
}

// ClientOptions 客户端配置选项
type ClientOptions struct {
	HeartbeatInterval time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	MaxRetries        int
	Compression       bool
}

// DefaultClientOptions 返回默认客户端配置
func DefaultClientOptions() *ClientOptions {
	return &ClientOptions{
		HeartbeatInterval: 30 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		MaxRetries:        3,
		Compression:       false,
	}
}

// ClientType 客户端类型
type ClientType int

const (
	// WebSocketClientType WebSocket客户端类型
	WebSocketClientType ClientType = iota

	// QUICClientType QUIC客户端类型
	QUICClientType
)

// NewClient 创建一个新的客户端
func NewClient(ctx context.Context, cfg *config.Config, clientType ClientType) (Client, error) {
	ctx, span := ob.StartSpan(ctx, "client.NewClient")
	defer span.End()

	// 创建相应类型的客户端
	switch clientType {
	case WebSocketClientType:
		logger.Info("Creating WebSocket client",
			zap.String("endpoint", cfg.WebSocket.Endpoint),
			zap.Uint64("userID", cfg.Client.UserID),
			zap.Uint64("liveID", cfg.Client.LiveID))
		return NewWebSocketClient(ctx, cfg)

	case QUICClientType:
		logger.Info("Creating QUIC client",
			zap.String("addr", cfg.Distributor.QUIC.Addr),
			zap.Uint64("userID", cfg.Client.UserID),
			zap.Uint64("liveID", cfg.Client.LiveID))
		return NewQUICClient(ctx, cfg)

	default:
		logger.Error("Unknown client type", zap.Int("type", int(clientType)))
		return nil, ErrUnsupportedClientType
	}
}
