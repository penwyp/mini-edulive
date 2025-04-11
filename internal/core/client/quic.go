// client/connection/quic.go
package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability" // 引入可观测性包
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/quic-go/quic-go"
	"go.opentelemetry.io/otel/attribute" // 用于添加 Span 属性
	"go.opentelemetry.io/otel/trace"     // 用于 Span 操作
	"go.uber.org/zap"
)

// QuicClient 表示 QUIC 客户端
type QuicClient struct {
	config    *config.Config
	conn      quic.Connection
	stream    quic.Stream
	liveID    uint64
	userID    uint64
	userName  string
	mutex     sync.RWMutex // 保护连接状态
	isClosed  bool
	tlsConfig *tls.Config
}

// NewQuicClient 创建新的 QUIC 客户端
func NewQuicClient(spanCtx context.Context, cfg *config.Config) (*QuicClient, error) {
	// 创建根 Span 追踪 QUIC 客户端初始化
	ctx, span := observability.StartSpan(spanCtx, "quic.NewQuicClient",
		trace.WithAttributes(
			attribute.String("addr", cfg.Distributor.QUIC.Addr),
			attribute.Int64("user_id", int64(cfg.Client.UserID)),
			attribute.Int64("live_id", int64(cfg.Client.LiveID)),
		))
	defer span.End()

	// 加载 TLS 证书
	startTime := time.Now()
	tlsConfig := util.GenerateTLSConfig(cfg.Distributor.QUIC.CertFile, cfg.Distributor.QUIC.KeyFile)
	observability.RecordLatency(ctx, "tls.GenerateTLSConfig", time.Since(startTime))

	// 建立 QUIC 连接
	startTime = time.Now()
	conn, err := quic.DialAddr(ctx, cfg.Distributor.QUIC.Addr, tlsConfig, &quic.Config{
		KeepAlivePeriod: 30 * time.Second, // 保持连接活跃
	})
	if err != nil {
		logger.Error("Failed to dial QUIC", zap.Error(err),
			zap.String("certFile", cfg.Distributor.QUIC.CertFile),
			zap.String("keyFile", cfg.Distributor.QUIC.KeyFile))
		observability.RecordError(span, err, "quic.NewQuicClient")
		return nil, fmt.Errorf("quic dial failed: %w", err)
	}
	observability.RecordLatency(ctx, "quic.DialAddr", time.Since(startTime))

	// 打开数据流
	startTime = time.Now()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		conn.CloseWithError(0, "failed to open stream")
		logger.Error("Failed to open QUIC stream", zap.Error(err))
		observability.RecordError(span, err, "quic.NewQuicClient")
		return nil, fmt.Errorf("open stream failed: %w", err)
	}
	observability.RecordLatency(ctx, "quic.OpenStreamSync", time.Since(startTime))

	client := &QuicClient{
		config:    cfg,
		conn:      conn,
		stream:    stream,
		userID:    cfg.Client.UserID,
		userName:  cfg.Client.UserName,
		liveID:    cfg.Client.LiveID,
		isClosed:  false,
		tlsConfig: tlsConfig,
	}

	// 发送初始化消息
	err = client.sendInitMessage(ctx)
	if err != nil {
		client.Close(ctx)
		logger.Error("Failed to send init message", zap.Error(err))
		observability.RecordError(span, err, "quic.NewQuicClient")
		return nil, fmt.Errorf("send init message failed: %w", err)
	}

	logger.Info("QUIC client initialized",
		zap.String("addr", cfg.Distributor.QUIC.Addr),
		zap.Uint64("userID", client.userID),
		zap.String("userName", client.userName),
		zap.Uint64("liveID", client.liveID))
	return client, nil
}

// sendInitMessage 发送初始化消息
func (c *QuicClient) sendInitMessage(spanCtx context.Context) error {
	// 创建 Span 追踪初始化消息发送
	ctx, span := observability.StartSpan(spanCtx, "quic.sendInitMessage",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
			attribute.String("user_name", c.userName),
		))
	defer span.End()

	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		err := fmt.Errorf("connection is closed")
		observability.RecordError(span, err, "quic.sendInitMessage")
		return err
	}
	stream := c.stream
	c.mutex.RUnlock()

	// 创建初始化消息
	msg := &protocol.BulletMessage{
		Magic:     protocol.MagicNumber,
		Version:   protocol.CurrentVersion,
		Type:      protocol.TypeBullet,
		Timestamp: time.Now().UnixMilli(),
		UserID:    c.userID,
		LiveID:    c.liveID,
		UserName:  c.userName,
		Content:   "",
	}

	startTime := time.Now()
	data, err := msg.Encode(c.config.Performance.BulletCompression)
	if err != nil {
		observability.RecordError(span, err, "quic.sendInitMessage")
		return fmt.Errorf("encode init message failed: %w", err)
	}
	observability.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	startTime = time.Now()
	_, err = stream.Write(data)
	if err != nil {
		observability.RecordError(span, err, "quic.sendInitMessage")
		return fmt.Errorf("write init message failed: %w", err)
	}
	observability.RecordLatency(ctx, "quic.Write", time.Since(startTime))

	logger.Debug("QUIC init message sent",
		zap.Uint64("liveID", c.liveID),
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName))
	return nil
}

// Receive 处理接收 QUIC 消息
func (c *QuicClient) Receive(spanCtx context.Context) error {
	// 创建根 Span 追踪接收过程
	ctx, span := observability.StartSpan(spanCtx, "quic.Receive",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
		))
	defer span.End()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context canceled, stopping QUIC receive")
			span.AddEvent("context_canceled")
			return nil
		default:
			// 创建子 Span 追踪单次消息接收
			msgCtx, msgSpan := observability.StartSpan(ctx, "quic.receiveMessage")
			startTime := time.Now()

			data, err := c.readStream(ctx)
			if err != nil {
				logger.Warn("Failed to read QUIC stream", zap.Error(err))
				observability.RecordError(msgSpan, err, "quic.receiveMessage")
				msgSpan.End()
				return fmt.Errorf("read stream: %w", err)
			}
			observability.RecordLatency(msgCtx, "quic.readStream", time.Since(startTime))
			logger.Debug("Read QUIC stream", zap.Int("bytes", len(data)))

			if len(data) == 0 {
				logger.Debug("Received empty data, skipping")
				msgSpan.AddEvent("empty_data")
				msgSpan.End()
				continue
			}

			reader := bytes.NewReader(data)
			for {
				var msgLen uint32
				if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
					if err == io.EOF {
						logger.Debug("Reached end of data")
						msgSpan.AddEvent("end_of_data")
						break
					}
					logger.Warn("Failed to read message length", zap.Error(err))
					observability.RecordError(msgSpan, err, "quic.receiveMessage")
					msgSpan.End()
					return fmt.Errorf("read message length: %w", err)
				}

				msgData := make([]byte, msgLen)
				if _, err := io.ReadFull(reader, msgData); err != nil {
					logger.Warn("Failed to read message data", zap.Error(err))
					observability.RecordError(msgSpan, err, "quic.receiveMessage")
					msgSpan.End()
					return fmt.Errorf("read message data: %w", err)
				}

				bullet, err := protocol.Decode(msgData)
				if err != nil {
					logger.Warn("Failed to decode bullet message", zap.Error(err))
					observability.RecordError(msgSpan, err, "quic.receiveMessage")
					continue
				}

				msgSpan.SetAttributes(
					attribute.Int64("bullet_user_id", int64(bullet.UserID)),
					attribute.Int64("bullet_live_id", int64(bullet.LiveID)),
					attribute.String("bullet_user_name", bullet.UserName),
					attribute.String("bullet_content", bullet.Content),
				)

				if bullet.Type == protocol.TypeBullet {
					colorFn := util.GetColorFunc(bullet.Color)
					colorFn("%s (%d): %s\n", bullet.UserName, bullet.UserID, bullet.Content)
				}
				bullet.Release()
			}
			observability.RecordLatency(msgCtx, "quic.processMessages", time.Since(startTime))
			msgSpan.End()
		}
	}
}

// readStream 从 QUIC 流中读取数据
func (c *QuicClient) readStream(spanCtx context.Context) ([]byte, error) {
	// 创建 Span 追踪流读取
	ctx, span := observability.StartSpan(spanCtx, "quic.readStream")
	defer span.End()

	var buf bytes.Buffer
	chunk := make([]byte, 1024)
	for {
		startTime := time.Now()
		err := c.stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			observability.RecordError(span, err, "quic.readStream")
			return nil, fmt.Errorf("set read deadline: %w", err)
		}

		n, err := c.stream.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		span.SetAttributes(attribute.Int("bytes_read", buf.Len()))
		if err != nil {
			if err == io.EOF {
				logger.Debug("QUIC stream EOF", zap.Int("total_bytes", buf.Len()))
				span.AddEvent("stream_eof")
				observability.RecordLatency(ctx, "quic.Read", time.Since(startTime))
				return buf.Bytes(), nil
			}
			if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
				logger.Debug("QUIC stream read timeout", zap.Int("total_bytes", buf.Len()))
				span.AddEvent("read_timeout")
				observability.RecordLatency(ctx, "quic.Read", time.Since(startTime))
				return buf.Bytes(), nil
			}
			observability.RecordError(span, err, "quic.readStream")
			return nil, fmt.Errorf("read chunk: %w", err)
		}
		observability.RecordLatency(ctx, "quic.ReadChunk", time.Since(startTime))
		if n < len(chunk) {
			logger.Debug("QUIC stream read complete", zap.Int("total_bytes", buf.Len()))
			span.AddEvent("read_complete")
			return buf.Bytes(), nil
		}
	}
}

// Close 关闭 QUIC 连接
func (c *QuicClient) Close(spanCtx context.Context) {
	// 创建 Span 追踪连接关闭
	ctx, span := observability.StartSpan(spanCtx, "quic.Close",
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
	stream := c.stream
	conn := c.conn
	c.mutex.Unlock()

	startTime := time.Now()
	if stream != nil {
		stream.Close()
	}
	if conn != nil {
		conn.CloseWithError(0, "client closed")
	}
	observability.RecordLatency(ctx, "quic.Close", time.Since(startTime))

	logger.Info("QUIC connection closed",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName),
		zap.Uint64("liveID", c.liveID))
}
