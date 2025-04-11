package connection

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"github.com/fatih/color"
	"github.com/penwyp/mini-edulive/pkg/util"
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
		Magic:     protocol.MagicNumber,
		Version:   protocol.CurrentVersion,
		Type:      protocol.TypeBullet,
		Timestamp: time.Now().UnixMilli(),
		UserID:    c.userID,
		LiveID:    c.liveID,
		UserName:  c.userName,
		Content:   "",
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

			// 解析多条弹幕消息
			reader := bytes.NewReader(data)
			for {
				// 读取消息长度（4 字节）
				var msgLen uint32
				if err := binary.Read(reader, binary.BigEndian, &msgLen); err != nil {
					if err == io.EOF {
						logger.Debug("Reached end of data")
						break
					}
					logger.Warn("Failed to read message length", zap.Error(err))
					return fmt.Errorf("read message length: %w", err)
				}

				// 读取消息内容
				msgData := make([]byte, msgLen)
				if _, err := io.ReadFull(reader, msgData); err != nil {
					logger.Warn("Failed to read message data", zap.Error(err))
					return fmt.Errorf("read message data: %w", err)
				}

				// 解码弹幕消息
				bullet, err := protocol.Decode(msgData)
				if err != nil {
					logger.Warn("Failed to decode bullet message", zap.Error(err))
					continue
				}

				if bullet.Type == protocol.TypeBullet {
					// 根据颜色字段选择彩色输出
					colorFn := getColorFunc(bullet.Color)
					colorFn("%s (%d): %s\n", bullet.UserName, bullet.UserID, bullet.Content)
				}
				//logger.Info("Received bullet",
				//	zap.Uint64("liveID", bullet.LiveID),
				//	zap.Uint64("userID", bullet.UserID),
				//	zap.String("username", bullet.UserName),
				//	zap.String("content", bullet.Content),
				//	zap.Int64("timestamp", bullet.Timestamp))
			}
		}
	}
}

// readStream 从 QUIC 流中读取数据
func (c *QuicClient) readStream() ([]byte, error) {
	var buf bytes.Buffer
	chunk := make([]byte, 1024)
	for {
		// 设置读取超时
		err := c.stream.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err != nil {
			return nil, fmt.Errorf("set read deadline: %w", err)
		}

		n, err := c.stream.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if err != nil {
			if err == io.EOF {
				logger.Debug("QUIC stream EOF", zap.Int("total_bytes", buf.Len()))
				return buf.Bytes(), nil
			}
			if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
				logger.Debug("QUIC stream read timeout", zap.Int("total_bytes", buf.Len()))
				return buf.Bytes(), nil
			}
			return nil, fmt.Errorf("read chunk: %w", err)
		}
		if n < len(chunk) {
			logger.Debug("QUIC stream read complete", zap.Int("total_bytes", buf.Len()))
			return buf.Bytes(), nil
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

// getColorFunc 根据颜色字符串返回对应的彩色打印函数
func getColorFunc(colorStr string) func(string, ...interface{}) (n int, err error) {
	switch colorStr {
	case "red":
		return color.New(color.FgRed).Printf
	case "green":
		return color.New(color.FgGreen).Printf
	case "blue":
		return color.New(color.FgBlue).Printf
	case "yellow":
		return color.New(color.FgYellow).Printf
	case "cyan":
		return color.New(color.FgCyan).Printf
	case "magenta":
		return color.New(color.FgMagenta).Printf
	default:
		return color.New(color.FgWhite).Printf
	}
}
