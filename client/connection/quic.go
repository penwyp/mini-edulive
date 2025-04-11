// client/connection/quic.go
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

type QuicClient struct {
	config      *config.Config
	session     quic.Connection
	stream      quic.Stream
	bulletCache *BulletCache
	mutex       sync.RWMutex
	wg          sync.WaitGroup
}

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
		return nil
	}
	return c.bullets
}

func (c *BulletCache) Update(bullets []*protocol.BulletMessage) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.bullets = bullets[:min(len(bullets), 10000)]
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

	// 发送初始化消息（包含 UserID 和 LiveID）
	msg := &protocol.BulletMessage{
		Magic:      protocol.MagicNumber,
		Version:    protocol.CurrentVersion,
		Type:       protocol.TypeHeartbeat, // 使用心跳类型作为初始化
		Timestamp:  time.Now().UnixMilli(),
		UserID:     cfg.Client.UserID,
		LiveID:     cfg.Client.LiveID,
		ContentLen: 0,
		Content:    "",
	}
	data, err := msg.Encode()
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
					zap.Uint64("liveID", bullet.LiveID),
					zap.Uint64("userID", bullet.UserID),
					zap.String("content", bullet.Content))
			}
		}
	}
}

func (c *QuicClient) readStream() ([]byte, error) {
	buf := make([]byte, 1024)
	n, err := c.stream.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (c *QuicClient) decompressBullets(data []byte) ([]*protocol.BulletMessage, error) {
	r, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var bullets []*protocol.BulletMessage
	decoder := msgp.NewReader(r)
	for {
		bullet := &protocol.BulletMessage{}
		err := bullet.DecodeMsg(decoder)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		bullets = append(bullets, bullet)
	}
	return bullets, nil
}

func (c *QuicClient) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stream.Close()
	c.session.CloseWithError(0, "closed")
	c.wg.Wait()
}
