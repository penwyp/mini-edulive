package connection

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
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
	tlsConfig, err := loadTLSConfig(cfg.Distributor.QUIC.CertFile)
	if err != nil {
		logger.Error("Failed to load TLS config", zap.Error(err))
		return nil, fmt.Errorf("load TLS config failed: %w", err)
	}

	// 建立 QUIC 连接
	conn, err := quic.DialAddr(context.Background(), cfg.Distributor.QUIC.Addr, tlsConfig, &quic.Config{
		KeepAlivePeriod: 30 * time.Second, // 保持连接活跃
	})
	if err != nil {
		logger.Error("Failed to dial QUIC", zap.Error(err))
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

// loadTLSConfig 加载 TLS 证书
func loadTLSConfig(certFile string) (*tls.Config, error) {
	// 读取证书
	certBytes, err := ioutil.ReadFile(certFile)
	if err != nil {
		return nil, fmt.Errorf("read cert file failed: %w", err)
	}

	// 创建证书池
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certBytes) {
		return nil, fmt.Errorf("append cert to pool failed")
	}

	return &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: false, // 生产环境应设置为 false，确保证书验证
		NextProtos:         []string{"quic-edulive"},
	}, nil
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

// Receive 接收弹幕消息
func (c *QuicClient) Receive(ctx context.Context) error {
	buffer := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			logger.Info("QUIC receive stopped due to context cancellation")
			return nil
		default:
			c.mutex.RLock()
			if c.isClosed {
				c.mutex.RUnlock()
				return fmt.Errorf("connection is closed")
			}
			stream := c.stream
			c.mutex.RUnlock()

			// 设置读取超时
			err := stream.SetReadDeadline(time.Now().Add(10 * time.Second))
			if err != nil {
				logger.Warn("Failed to set read deadline", zap.Error(err))
				return fmt.Errorf("set read deadline failed: %w", err)
			}

			n, err := stream.Read(buffer)
			if err != nil {
				logger.Warn("Failed to read from QUIC stream", zap.Error(err))
				return fmt.Errorf("read stream failed: %w", err)
			}

			// 解码收到的消息
			data := buffer[:n]
			msg, err := protocol.Decode(data)
			if err != nil {
				logger.Warn("Failed to decode QUIC message", zap.Error(err))
				continue
			}

			if msg.Type == protocol.TypeBullet {
				logger.Info("Received bullet",
					zap.Uint64("liveID", msg.LiveID),
					zap.Uint64("userID", msg.UserID),
					zap.String("userName", c.userName),
					zap.String("content", msg.Content))
			} else {
				logger.Debug("Received non-bullet message",
					zap.Uint8("type", msg.Type),
					zap.Uint64("userID", msg.UserID),
					zap.String("userName", c.userName))
			}
		}
	}
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
