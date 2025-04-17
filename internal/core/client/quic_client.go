package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/quic-go/quic-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// QUICClient QUIC客户端实现
type QUICClient struct {
	config       *config.Config
	options      *ClientOptions
	conn         quic.Connection
	stream       quic.Stream
	liveID       uint64
	userID       uint64
	userName     string
	mutex        sync.RWMutex
	isClosed     bool
	tlsConfig    *tls.Config
	bulletBuffer []*protocol.BulletMessage // 用于存储待处理的弹幕
	bufferMutex  sync.Mutex                // 用于保护弹幕缓冲区
}

// NewQUICClient 创建一个新的QUIC客户端
func NewQUICClient(ctx context.Context, cfg *config.Config) (*QUICClient, error) {
	ctx, span := ob.StartSpan(ctx, "quic.NewQuicClient",
		trace.WithAttributes(
			attribute.String("addr", cfg.Distributor.QUIC.Addr),
			attribute.Int64("user_id", int64(cfg.Client.UserID)),
			attribute.Int64("live_id", int64(cfg.Client.LiveID)),
		))
	defer span.End()

	// 创建默认选项
	options := DefaultClientOptions()
	options.Compression = cfg.Performance.BulletCompression

	// 生成TLS配置
	startTime := time.Now()
	tlsConfig := util.GenerateTLSConfig(cfg.Distributor.QUIC.CertFile, cfg.Distributor.QUIC.KeyFile)
	if tlsConfig == nil {
		err := ErrTLSConfigFailed
		logger.Error("Failed to generate TLS config", zap.Error(err))
		ob.RecordError(span, err, "quic.NewQuicClient")
		ob.QUICConnectionErrors.WithLabelValues("client", "setup").Inc()
		return nil, err
	}
	ob.RecordLatency(ctx, "tls.GenerateTLSConfig", time.Since(startTime))

	// 建立QUIC连接
	startTime = time.Now()
	conn, err := quic.DialAddr(ctx, cfg.Distributor.QUIC.Addr, tlsConfig, &quic.Config{
		KeepAlivePeriod: options.HeartbeatInterval,
	})
	if err != nil {
		logger.Error("Failed to dial QUIC", zap.Error(err),
			zap.String("certFile", cfg.Distributor.QUIC.CertFile),
			zap.String("keyFile", cfg.Distributor.QUIC.KeyFile))
		ob.RecordError(span, err, "quic.NewQuicClient")
		ob.QUICConnectionErrors.WithLabelValues("client", "accept").Inc()
		return nil, NewConnectionError("dial", err)
	}
	ob.ActiveQUICConnections.WithLabelValues("client").Inc()
	ob.RecordLatency(ctx, "quic.DialAddr", time.Since(startTime))

	// 打开流
	startTime = time.Now()
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		// 关闭连接
		conn.CloseWithError(0, "failed to open stream")
		logger.Error("Failed to open QUIC stream", zap.Error(err))
		ob.RecordError(span, err, "quic.NewQuicClient")
		ob.QUICConnectionErrors.WithLabelValues("client", "open_stream").Inc()
		ob.ActiveQUICConnections.WithLabelValues("client").Dec()
		return nil, NewConnectionError("open_stream", err)
	}
	ob.RecordLatency(ctx, "quic.OpenStreamSync", time.Since(startTime))

	client := &QUICClient{
		config:       cfg,
		options:      options,
		conn:         conn,
		stream:       stream,
		userID:       cfg.Client.UserID,
		userName:     cfg.Client.UserName,
		liveID:       cfg.Client.LiveID,
		isClosed:     false,
		tlsConfig:    tlsConfig,
		bulletBuffer: make([]*protocol.BulletMessage, 0),
	}

	// 发送初始化消息
	if err := client.sendInitMessage(ctx); err != nil {
		client.Close(ctx)
		logger.Error("Failed to send init message", zap.Error(err))
		ob.RecordError(span, err, "quic.NewQuicClient")
		ob.QUICConnectionErrors.WithLabelValues("client", "write").Inc()
		return nil, err
	}

	logger.Info("QUIC client initialized",
		zap.String("addr", cfg.Distributor.QUIC.Addr),
		zap.Uint64("userID", client.userID),
		zap.String("userName", client.userName),
		zap.Uint64("liveID", client.liveID))
	return client, nil
}

// Send 发送消息
func (c *QUICClient) Send(ctx context.Context, content string) error {
	// QUIC客户端不支持发送弹幕
	return fmt.Errorf("QUIC client does not support sending bullets")
}

// CreateRoom 创建房间
func (c *QUICClient) CreateRoom(ctx context.Context, liveID, userID uint64, userName string) error {
	// QUIC客户端不支持创建房间
	return fmt.Errorf("QUIC client does not support creating rooms")
}

// CheckRoom 检查房间
func (c *QUICClient) CheckRoom(ctx context.Context, liveID, userID uint64, userName string) error {
	// QUIC客户端不支持检查房间
	return fmt.Errorf("QUIC client does not support checking rooms")
}

// StartHeartbeat 启动心跳
func (c *QUICClient) StartHeartbeat(ctx context.Context) error {
	// QUIC协议内置了心跳机制，不需要额外实现
	return nil
}

// sendInitMessage 发送初始化消息
func (c *QUICClient) sendInitMessage(ctx context.Context) error {
	ctx, span := ob.StartSpan(ctx, "quic.sendInitMessage",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
			attribute.String("user_name", c.userName),
		))
	defer span.End()

	// 检查连接状态
	if err := c.checkConnection(); err != nil {
		ob.RecordError(span, err, "quic.sendInitMessage")
		return err
	}

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

	// 编码消息
	startTime := time.Now()
	data, err := msg.Encode(c.options.Compression)
	if err != nil {
		ob.RecordError(span, err, "quic.sendInitMessage")
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return NewProtocolError("encode", err)
	}
	ob.RecordLatency(ctx, "protocol.Encode", time.Since(startTime))

	// 发送消息
	startTime = time.Now()
	_, err = c.stream.Write(data)
	if err != nil {
		ob.RecordError(span, err, "quic.sendInitMessage")
		ob.QUICConnectionErrors.WithLabelValues("client", "write").Inc()
		return NewConnectionError("write", err)
	}
	ob.RecordLatency(ctx, "quic.Write", time.Since(startTime))

	logger.Debug("QUIC init message sent",
		zap.Uint64("liveID", c.liveID),
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName))
	return nil
}

// Receive 接收并处理QUIC消息
func (c *QUICClient) Receive(ctx context.Context) error {
	ctx, span := ob.StartSpan(ctx, "quic.Receive",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(c.userID)),
			attribute.Int64("live_id", int64(c.liveID)),
		))
	defer span.End()

	// 启动一个定时器，定期处理缓冲区中的弹幕
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.processBulletBuffer(ctx)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Context canceled, stopping QUIC receive")
			span.AddEvent("context_canceled")
			return nil
		default:
			// 接收单条消息
			msgCtx, msgSpan := ob.StartSpan(ctx, "quic.receiveMessage")
			startTime := time.Now()

			// 读取数据
			data, err := c.readStream(msgCtx)
			if err != nil {
				logger.Warn("Failed to read QUIC stream", zap.Error(err))
				ob.RecordError(msgSpan, err, "quic.receiveMessage")
				ob.QUICConnectionErrors.WithLabelValues("client", "read").Inc()
				msgSpan.End()
				return err
			}
			ob.RecordLatency(msgCtx, "quic.readStream", time.Since(startTime))

			// 跳过空数据
			if len(data) == 0 {
				logger.Debug("Received empty data, skipping")
				msgSpan.AddEvent("empty_data")
				msgSpan.End()
				continue
			}

			// 处理数据
			if err := c.processMessageData(msgCtx, data); err != nil {
				logger.Warn("Failed to process message data", zap.Error(err))
				ob.RecordError(msgSpan, err, "quic.receiveMessage")
				msgSpan.End()
				continue
			}

			ob.RecordLatency(msgCtx, "quic.processMessages", time.Since(startTime))
			msgSpan.End()
		}
	}
}

// processMessageData 处理消息数据
func (c *QUICClient) processMessageData(ctx context.Context, data []byte) error {
	ctx, span := ob.StartSpan(ctx, "quic.processMessageData")
	defer span.End()

	reader := bytes.NewReader(data)
	for {
		// 读取消息长度
		var msgLen uint32
		if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
			if err == io.EOF {
				logger.Debug("Reached end of data")
				span.AddEvent("end_of_data")
				break
			}
			ob.RecordError(span, err, "quic.processMessageData")
			ob.QUICConnectionErrors.WithLabelValues("client", "read").Inc()
			return NewConnectionError("read_length", err)
		}

		// 读取消息数据
		msgData := make([]byte, msgLen)
		if _, err := io.ReadFull(reader, msgData); err != nil {
			ob.RecordError(span, err, "quic.processMessageData")
			ob.QUICConnectionErrors.WithLabelValues("client", "read").Inc()
			return NewConnectionError("read_data", err)
		}

		// 解码弹幕消息
		bullet, err := protocol.Decode(msgData)
		if err != nil {
			logger.Warn("Failed to decode bullet message", zap.Error(err))
			ob.RecordError(span, err, "quic.processMessageData")
			ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
			continue
		}

		// 记录属性
		span.SetAttributes(
			attribute.Int64("bullet_user_id", int64(bullet.UserID)),
			attribute.Int64("bullet_live_id", int64(bullet.LiveID)),
			attribute.String("bullet_user_name", bullet.UserName),
			attribute.String("bullet_content", bullet.Content),
		)

		// 处理弹幕
		if bullet.Type == protocol.TypeBullet {
			// 将弹幕添加到缓冲区
			c.bufferMutex.Lock()
			c.bulletBuffer = append(c.bulletBuffer, bullet)
			c.bufferMutex.Unlock()

			ob.BulletMessagesProcessed.WithLabelValues("client", "buffered").Inc()
		} else {
			// 非弹幕消息直接释放
			bullet.Release()
		}
	}

	return nil
}

// processBulletBuffer 处理弹幕缓冲区
func (c *QUICClient) processBulletBuffer(ctx context.Context) {
	ctx, span := ob.StartSpan(ctx, "quic.processBulletBuffer")
	defer span.End()

	c.bufferMutex.Lock()
	defer c.bufferMutex.Unlock()

	// 如果缓冲区为空，则直接返回
	if len(c.bulletBuffer) == 0 {
		span.AddEvent("empty_buffer")
		return
	}

	// 按时间戳排序弹幕
	sort.Slice(c.bulletBuffer, func(i, j int) bool {
		return c.bulletBuffer[i].Timestamp < c.bulletBuffer[j].Timestamp
	})

	// 输出排序后的弹幕，每行增加TAB
	for i, bullet := range c.bulletBuffer {
		// 创建缩进
		indent := strings.Repeat("\t", i)

		// 输出弹幕
		colorFn := util.GetColorFunc(bullet.Color)
		colorFn("%s%s (%d): %s\n", indent, bullet.UserName, bullet.UserID, bullet.Content)

		// 释放弹幕资源
		bullet.Release()

		ob.BulletMessagesProcessed.WithLabelValues("client", "success").Inc()
	}

	// 清空缓冲区
	c.bulletBuffer = make([]*protocol.BulletMessage, 0)

	span.SetAttributes(attribute.Int("bullets_processed", len(c.bulletBuffer)))
}

// readStream 从QUIC流中读取数据
func (c *QUICClient) readStream(ctx context.Context) ([]byte, error) {
	ctx, span := ob.StartSpan(ctx, "quic.readStream")
	defer span.End()

	// 检查连接状态
	if err := c.checkConnection(); err != nil {
		ob.RecordError(span, err, "quic.readStream")
		return nil, err
	}

	var buf bytes.Buffer
	chunk := make([]byte, 1024)

	for {
		startTime := time.Now()

		// 设置读取超时
		err := c.stream.SetReadDeadline(time.Now().Add(c.options.ReadTimeout))
		if err != nil {
			ob.RecordError(span, err, "quic.readStream")
			ob.QUICConnectionErrors.WithLabelValues("client", "read").Inc()
			return nil, NewConnectionError("set_deadline", err)
		}

		// 读取数据块
		n, err := c.stream.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}

		span.SetAttributes(attribute.Int("bytes_read", buf.Len()))

		if err != nil {
			// 正常结束
			if err == io.EOF {
				logger.Debug("QUIC stream EOF", zap.Int("total_bytes", buf.Len()))
				span.AddEvent("stream_eof")
				ob.RecordLatency(ctx, "quic.Read", time.Since(startTime))
				return buf.Bytes(), nil
			}

			// 超时
			if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
				logger.Debug("QUIC stream read timeout", zap.Int("total_bytes", buf.Len()))
				span.AddEvent("read_timeout")
				ob.RecordLatency(ctx, "quic.Read", time.Since(startTime))
				return buf.Bytes(), nil
			}

			// 其他错误
			ob.RecordError(span, err, "quic.readStream")
			ob.QUICConnectionErrors.WithLabelValues("client", "read").Inc()
			return nil, NewConnectionError("read", err)
		}

		ob.RecordLatency(ctx, "quic.ReadChunk", time.Since(startTime))

		// 如果读取的数据不足一个块，说明已经读完
		if n < len(chunk) {
			logger.Debug("QUIC stream read complete", zap.Int("total_bytes", buf.Len()))
			span.AddEvent("read_complete")
			return buf.Bytes(), nil
		}
	}
}

// Close 关闭QUIC连接
func (c *QUICClient) Close(ctx context.Context) {
	ctx, span := ob.StartSpan(ctx, "quic.Close",
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

	// 关闭流
	if stream != nil {
		err := stream.Close()
		if err != nil {
			logger.Error("Failed to close QUIC stream", zap.Error(err))
			ob.RecordError(span, err, "quic.Close")
			ob.QUICConnectionErrors.WithLabelValues("client", "close").Inc()
		}
	}

	// 关闭连接
	if conn != nil {
		err := conn.CloseWithError(0, "client closed")
		if err != nil {
			logger.Error("Failed to close QUIC connection", zap.Error(err))
			ob.RecordError(span, err, "quic.Close")
			ob.QUICConnectionErrors.WithLabelValues("client", "close").Inc()
		}
	}

	ob.ActiveQUICConnections.WithLabelValues("client").Dec()
	ob.RecordLatency(ctx, "quic.Close", time.Since(startTime))

	logger.Info("QUIC connection closed",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName),
		zap.Uint64("liveID", c.liveID))
}

// checkConnection 检查连接状态
func (c *QUICClient) checkConnection() error {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.isClosed {
		return ErrConnectionClosed
	}
	return nil
}
