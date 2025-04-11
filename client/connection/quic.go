package connection

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/tinylib/msgp/msgp"
	"io"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/quic-go/quic-go"
	"go.uber.org/zap"
)

// QuicClient 表示 QUIC 客户端
type QuicClient struct {
	cfg       *config.Config
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
func NewQuicClient(cfg *config.Config) (*QuicClient, error) {
	// 加载 TLS 证书
	tlsConfig := util.GenerateTLSConfig(cfg.Distributor.QUIC.CertFile, cfg.Distributor.QUIC.KeyFile)

	// 建立 QUIC 连接
	conn, err := quic.DialAddr(context.Background(), cfg.Distributor.QUIC.Addr, tlsConfig, &quic.Config{
		KeepAlivePeriod: 30 * time.Second, // 保持连接活跃
	})
	if err != nil {
		logger.Error("Failed to dial QUIC", zap.Error(err),
			zap.String("certFile", cfg.Distributor.QUIC.CertFile),
			zap.String("keyFile", cfg.Distributor.QUIC.KeyFile),
		)
		return nil, fmt.Errorf("quic dial failed: %w", err)
	}

	// 打开数据流
	stream, err := conn.OpenStreamSync(context.Background())
	if err != nil {
		conn.CloseWithError(0, "failed to open stream")
		logger.Error("Failed to open QUIC stream", zap.Error(err))
		return nil, fmt.Errorf("open stream failed: %w", err)
	}

	client := &QuicClient{
		cfg:       cfg,
		conn:      conn,
		stream:    stream,
		userID:    cfg.Client.UserID,
		userName:  cfg.Client.UserName,
		liveID:    cfg.Client.LiveID,
		isClosed:  false,
		tlsConfig: tlsConfig,
	}

	// 发送初始化消息（携带 userID 和 liveID）
	err = client.sendInitMessage()
	if err != nil {
		client.Close()
		logger.Error("Failed to send init message", zap.Error(err))
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
func (c *QuicClient) sendInitMessage() error {
	c.mutex.RLock()
	if c.isClosed {
		c.mutex.RUnlock()
		return fmt.Errorf("connection is closed")
	}
	stream := c.stream
	c.mutex.RUnlock()

	// 创建初始化消息（复用 BulletMessage，类型为 TypeBullet）
	msg := &protocol.BulletMessage{
		Magic:      protocol.MagicNumber,
		Version:    protocol.CurrentVersion,
		Type:       protocol.TypeBullet,
		Timestamp:  time.Now().UnixMilli(),
		UserID:     c.userID,
		LiveID:     c.liveID,
		Username:   c.userName,
		ContentLen: 0,
		Content:    "",
	}

	data, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("encode init message failed: %w", err)
	}

	_, err = stream.Write(data)
	if err != nil {
		return fmt.Errorf("write init message failed: %w", err)
	}

	logger.Debug("QUIC init message sent",
		zap.Uint64("liveID", c.liveID),
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName))

	return nil
}

// Receive 处理接收 QUIC 消息
func (c *QuicClient) Receive(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			logger.Info("Context canceled, stopping QUIC receive")
			return nil
		default:
			// 读取 QUIC 流数据
			data, err := c.readStream()
			if err != nil {
				logger.Warn("Failed to read QUIC stream", zap.Error(err))
				return fmt.Errorf("read stream: %w", err)
			}
			logger.Debug("Read QUIC stream", zap.Int("bytes", len(data)))

			// 检查数据是否为空
			if len(data) == 0 {
				logger.Debug("Received empty data, skipping")
				continue
			}

			// 解压 zstd 数据
			reader, err := zstd.NewReader(bytes.NewReader(data))
			if err != nil {
				logger.Warn("Failed to create zstd reader", zap.Error(err))
				return fmt.Errorf("create zstd reader: %w", err)
			}
			defer reader.Close()

			// 读取解压后的数据
			var decompressedData bytes.Buffer
			n, err := io.Copy(&decompressedData, reader)
			if err != nil {
				logger.Warn("Failed to decompress zstd data", zap.Error(err))
				return fmt.Errorf("decompress zstd: %w", err)
			}
			logger.Debug("Decompressed data", zap.Int64("bytes", n))

			// 检查解压后的数据是否为空
			if n == 0 {
				logger.Debug("Decompressed data is empty, skipping")
				continue
			}

			// 使用 msgp 反序列化弹幕消息列表
			var bullets []*protocol.BulletMessage
			dec := msgp.NewReader(&decompressedData)
			for {
				var bullet protocol.BulletMessage
				err := bullet.DecodeMsg(dec)
				if err == io.EOF {
					logger.Debug("Reached end of msgp data")
					break // 所有消息解析完成
				}
				if err != nil {
					logger.Warn("Skipping invalid msgp message", zap.Error(err))
					continue // 跳过无效消息，而不是返回错误
				}
				bullets = append(bullets, &bullet)
			}

			// 检查是否解析到有效消息
			if len(bullets) == 0 {
				logger.Debug("No valid bullets parsed, skipping")
				continue
			}

			// 处理收到的弹幕消息
			for _, bullet := range bullets {
				logger.Info("Received bullet",
					zap.Uint64("liveID", bullet.LiveID),
					zap.Uint64("userID", bullet.UserID),
					zap.String("username", bullet.Username),
					zap.String("content", bullet.Content),
					zap.Int64("timestamp", bullet.Timestamp))
			}
		}
	}
}

// readStream 从 QUIC 流中读取数据
func (c *QuicClient) readStream() ([]byte, error) {
	var buf bytes.Buffer
	chunk := make([]byte, 1024)
	for {
		n, err := c.stream.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, err
		}
		buf.Write(chunk[:n])
		if err == io.EOF || n < len(chunk) {
			break
		}
	}
	return buf.Bytes(), nil
}

// Close 关闭 QUIC 连接
func (c *QuicClient) Close() {
	c.mutex.Lock()
	if c.isClosed {
		c.mutex.Unlock()
		return
	}
	c.isClosed = true
	stream := c.stream
	conn := c.conn
	c.mutex.Unlock()

	if stream != nil {
		stream.Close()
	}
	if conn != nil {
		conn.CloseWithError(0, "client closed")
	}

	logger.Info("QUIC connection closed",
		zap.Uint64("userID", c.userID),
		zap.String("userName", c.userName),
		zap.Uint64("liveID", c.liveID))
}
