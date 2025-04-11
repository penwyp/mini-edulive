package connection

import (
	"context"
	"fmt"
	"time"

	"github.com/coder/websocket"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.uber.org/zap"
)

// Client WebSocket 客户端
type Client struct {
	conn    *websocket.Conn
	config  *config.Config
	liveID  uint64
	userID  uint64
	filters *SensitiveFilter // 本地敏感词过滤器
}

// NewClient 创建 WebSocket 客户端
func NewClient(cfg *config.Config) (*Client, error) {
	conn, _, err := websocket.Dial(context.Background(), cfg.WebSocket.Endpoint, nil)
	if err != nil {
		return nil, err
	}

	// 初始化敏感词过滤器
	filters := NewSensitiveFilter()
	filters.AddWords([]string{"敏感词1", "敏感词2"}) // 示例敏感词，可从配置文件加载

	return &Client{
		conn:    conn,
		config:  cfg,
		liveID:  cfg.Client.LiveID,
		userID:  cfg.Client.UserID,
		filters: filters,
	}, nil
}

// SendBullet 发送弹幕消息
func (c *Client) SendBullet(content string) error {
	// 本地敏感词过滤
	if c.filters.Contains(content) {
		logger.Warn("Bullet contains sensitive words", zap.String("content", content))
		return nil // 直接丢弃，不发送
	}

	msg := protocol.NewBulletMessage(c.liveID, c.userID, content)
	data, err := msg.Encode()
	if err != nil {
		return err
	}

	err = c.conn.Write(context.Background(), websocket.MessageBinary, data)
	if err != nil {
		logger.Warn("Failed to send bullet", zap.Error(err))
		return err
	}
	logger.Info("Bullet sent", zap.Uint64("userID", c.userID), zap.String("content", content))
	return nil
}

// SendHeartbeat 发送心跳消息
func (c *Client) SendHeartbeat() error {
	msg := protocol.NewHeartbeatMessage(c.userID)
	data, err := msg.Encode()
	if err != nil {
		return err
	}

	err = c.conn.Write(context.Background(), websocket.MessageBinary, data)
	if err != nil {
		logger.Warn("Failed to send heartbeat", zap.Error(err))
		return err
	}
	logger.Debug("Heartbeat sent", zap.Uint64("userID", c.userID))
	return nil
}

// StartHeartbeat 启动心跳循环
func (c *Client) StartHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.SendHeartbeat(); err != nil {
				logger.Error("Heartbeat failed", zap.Error(err))
				return
			}
		}
	}
}

// Close 关闭连接
func (c *Client) Close() {
	c.conn.Close(websocket.StatusNormalClosure, "")
	logger.Info("WebSocket client closed", zap.Uint64("userID", c.userID))
}

// SensitiveFilter 敏感词过滤器（基于 Trie 树）
type SensitiveFilter struct {
	root *TrieNode
}

type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
}

func NewSensitiveFilter() *SensitiveFilter {
	return &SensitiveFilter{root: &TrieNode{children: make(map[rune]*TrieNode)}}
}

func (f *SensitiveFilter) AddWord(word string) {
	node := f.root
	for _, ch := range word {
		if _, ok := node.children[ch]; !ok {
			node.children[ch] = &TrieNode{children: make(map[rune]*TrieNode)}
		}
		node = node.children[ch]
	}
	node.isEnd = true
}

func (f *SensitiveFilter) AddWords(words []string) {
	for _, word := range words {
		f.AddWord(word)
	}
}

func (f *SensitiveFilter) Contains(content string) bool {
	runes := []rune(content)
	for i := 0; i < len(runes); i++ {
		node := f.root
		for j := i; j < len(runes); j++ {
			if next, ok := node.children[runes[j]]; ok {
				node = next
				if node.isEnd {
					return true
				}
			} else {
				break
			}
		}
	}
	return false
}

// CreateRoom 发送创建直播间请求
// CreateRoom 发送创建直播间请求
func (c *Client) CreateRoom(liveID, userID uint64) error {
	msg := protocol.NewCreateRoomMessage(liveID, userID)
	data, err := msg.Encode()
	if err != nil {
		return err
	}
	err = c.conn.Write(context.Background(), websocket.MessageBinary, data)
	if err != nil {
		return err
	}

	// 等待响应
	_, respData, err := c.conn.Read(context.Background())
	if err != nil {
		return err
	}
	respMsg, err := protocol.Decode(respData)
	if err != nil {
		return err
	}
	if respMsg.Type == protocol.TypeCreateRoom && respMsg.Content != "" {
		// 服务端返回错误消息
		logger.Error("Failed to create room",
			zap.String("error", respMsg.Content))
		return fmt.Errorf("create room failed: %s", respMsg.Content)
	}
	if respMsg.Type != protocol.TypeCreateRoom {
		return fmt.Errorf("unexpected response type: %d", respMsg.Type)
	}
	logger.Info("Received create room response",
		zap.Uint64("liveID", respMsg.LiveID),
		zap.Uint64("userID", respMsg.UserID))
	return nil
}
