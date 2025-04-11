package connection

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/quic-go/quic-go"
	"github.com/tinylib/msgp/msgp"
	"go.uber.org/zap"
)

// QuicClient QUIC 客户端，用于接收弹幕
type QuicClient struct {
	config      *config.Config
	session     quic.Connection
	stream      quic.Stream
	bulletCache *BulletCache // 本地弹幕缓存
	mutex       sync.RWMutex
	wg          sync.WaitGroup
}

// BulletCache 本地弹幕缓存
type BulletCache struct {
	bullets []*protocol.BulletMessage
	expiry  time.Time
	mutex   sync.RWMutex
}

func NewBulletCache() *BulletCache {
	return &BulletCache{
		bullets: make([]*protocol.BulletMessage, 0, 10000),
		expiry:  time.Now().Add(10 * time.Millisecond),
	}
}

func (c *BulletCache) GetBullets() []*protocol.BulletMessage {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if time.Now().After(c.expiry) {
		return nil // 缓存过期
	}
	return c.bullets
}

func (c *BulletCache) Update(bullets []*protocol.BulletMessage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.bullets = bullets[:min(len(bullets), 10000)] // 限制最大 10000 条
	c.expiry = time.Now().Add(10 * time.Millisecond)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// NewQuicClient 创建 QUIC 客户端
func NewQuicClient(cfg *config.Config) (*QuicClient, error) {
	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // 测试用，生产环境需使用正确证书
		NextProtos:         []string{"quic-edulive"},
	}
	session, err := quic.DialAddr(context.Background(), cfg.Distributor.QUIC.Addr, tlsConf, nil)
	if err != nil {
		return nil, err
	}

	stream, err := session.OpenStreamSync(context.Background())
	if err != nil {
		return nil, err
	}

	// 发送初始化消息（包含 UserID）
	msg := protocol.NewHeartbeatMessage(cfg.Client.UserID)
	data, err := msg.Encode() // 使用 Protobuf 格式发送初始化消息
	if err != nil {
		return nil, err
	}
	_, err = stream.Write(data)
	if err != nil {
		return nil, err
	}

	return &QuicClient{
		config:      cfg,
		session:     session,
		stream:      stream,
		bulletCache: NewBulletCache(),
	}, nil
}

// Receive 接收弹幕
func (c *QuicClient) Receive(ctx context.Context) {
	c.wg.Add(1)
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			data, err := c.readStream()
			if err != nil {
				logger.Warn("Failed to read QUIC stream", zap.Error(err))
				return
			}

			bullets, err := c.decompressBullets(data)
			if err != nil {
				logger.Warn("Failed to decompress bullets", zap.Error(err))
				continue
			}

			c.bulletCache.Update(bullets)
			for _, bullet := range bullets {
				logger.Info("Received bullet",
					zap.Uint64("userID", bullet.UserID),
					zap.String("content", bullet.Content))
				// TODO: 显示弹幕到界面
			}
		}
	}
}

// readStream 从 QUIC 流中读取数据
func (c *QuicClient) readStream() ([]byte, error) {
	buf := make([]byte, 1024*10) // 10KB 缓冲区
	n, err := c.stream.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// decompressBullets 解压并反序列化弹幕数据
func (c *QuicClient) decompressBullets(data []byte) ([]*protocol.BulletMessage, error) {
	// 解压 zstd 数据
	dec, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	// 读取解压后的数据
	var decompressed bytes.Buffer
	_, err = io.Copy(&decompressed, dec)
	if err != nil {
		return nil, err
	}

	// 使用 msgp 反序列化
	reader := msgp.NewReader(&decompressed)
	var bullets []*protocol.BulletMessage
	for {
		var bullet protocol.BulletMessage
		err := bullet.DecodeMsg(reader)
		if err != nil {
			if err == io.EOF {
				break // 读取完成
			}
			logger.Warn("Failed to decode bullet", zap.Error(err))
			continue
		}
		bullets = append(bullets, &bullet)
	}

	return bullets, nil
}

// Close 关闭 QUIC 连接
func (c *QuicClient) Close() {
	c.stream.Close()
	c.session.CloseWithError(0, "")
	c.wg.Wait()
	logger.Info("QUIC client closed", zap.Uint64("userID", c.config.Client.UserID))
}
