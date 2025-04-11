package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/klauspost/compress/zstd"
	"time"
)

// 全局 zstd 编码器和解码器（复用以提升性能）
var (
	zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	zstdDecoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
)

//go:generate msgp
const (
	MagicNumber    = 0xABCD
	CurrentVersion = 0x01
	TypeBullet     = 0x01 // 弹幕消息
	TypeHeartbeat  = 0x02 // 心跳消息
	TypeCreateRoom = 0x03 // 创建直播间消息
	TypeCheckRoom  = 0x04 // 检查房间存在性消息
)

// SerializedBullet 表示序列化后的弹幕内容
type SerializedBullet struct {
	Timestamp int64  `json:"timestamp"`
	UserID    uint64 `json:"user_id"`
	LiveID    uint64 `json:"live_id"`
	UserName  string `json:"username"`
	Content   string `json:"content"`
	Color     string `json:"color"` // 支持彩色显示
}

// BulletMessage 定义二进制协议结构体
//
//msgp:tuple BulletMessage
type BulletMessage struct {
	Magic     uint16 `msgp:"magic"`     // 魔数
	Version   uint8  `msgp:"version"`   // 版本号
	Type      uint8  `msgp:"type"`      // 消息类型
	Timestamp int64  `msgp:"timestamp"` // 时间戳
	UserID    uint64 `msgp:"user_id"`   // 用户ID
	LiveID    uint64 `msgp:"live_id"`   // 直播间ID
	UserName  string `msgp:"username"`  // 用户名
	Content   string `msgp:"content"`   // 内容
	Color     string `msgp:"color"`     // 颜色
}

// Encode 将 BulletMessage 编码为二进制数据，支持可选压缩
func (msg *BulletMessage) Encode(compress bool) ([]byte, error) {
	buf := new(bytes.Buffer)

	// 写入固定字段
	if err := binary.Write(buf, binary.BigEndian, msg.Magic); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Version); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Type); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.UserID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.LiveID); err != nil {
		return nil, err
	}

	// 写入 UserName（变长字段，前缀长度 + 内容）
	usernameBytes := []byte(msg.UserName)
	usernameLen := uint16(len(usernameBytes))
	if err := binary.Write(buf, binary.BigEndian, usernameLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, usernameBytes); err != nil {
		return nil, err
	}

	// 写入 Content（变长字段，前缀长度 + 内容）
	contentBytes := []byte(msg.Content)
	contentLen := uint16(len(contentBytes))
	if err := binary.Write(buf, binary.BigEndian, contentLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, contentBytes); err != nil {
		return nil, err
	}

	// 写入 Color（变长字段，前缀长度 + 内容）
	colorBytes := []byte(msg.Color)
	colorLen := uint16(len(colorBytes))
	if err := binary.Write(buf, binary.BigEndian, colorLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, colorBytes); err != nil {
		return nil, err
	}

	data := buf.Bytes()

	if compress {
		// 使用 zstd 压缩
		compressed := zstdEncoder.EncodeAll(data, nil)
		// 在压缩数据前添加一个标志位（1 表示压缩）
		result := append([]byte{1}, compressed...)
		return result, nil
	}

	// 未压缩数据前添加标志位（0 表示未压缩）
	return append([]byte{0}, data...), nil
}

// Decode 从二进制数据解码为 BulletMessage，支持可选压缩
func Decode(data []byte) (*BulletMessage, error) {
	if len(data) < 1 {
		return nil, errors.New("data too short")
	}

	// 读取压缩标志位
	isCompressed := data[0] == 1
	rawData := data[1:]

	if isCompressed {
		// 解压数据
		decompressed, err := zstdDecoder.DecodeAll(rawData, nil)
		if err != nil {
			return nil, errors.New("failed to decompress data")
		}
		rawData = decompressed
	}

	if len(rawData) < 20 {
		return nil, errors.New("data too short after decompression")
	}

	reader := bytes.NewReader(rawData)
	msg := &BulletMessage{}

	// 读取魔数
	if err := binary.Read(reader, binary.BigEndian, &msg.Magic); err != nil {
		return nil, err
	}
	if msg.Magic != MagicNumber {
		return nil, errors.New("invalid magic number")
	}

	// 读取版本
	if err := binary.Read(reader, binary.BigEndian, &msg.Version); err != nil {
		return nil, err
	}
	if msg.Version != CurrentVersion {
		return nil, errors.New("unsupported version")
	}

	// 读取类型
	if err := binary.Read(reader, binary.BigEndian, &msg.Type); err != nil {
		return nil, err
	}
	if msg.Type != TypeBullet &&
		msg.Type != TypeHeartbeat &&
		msg.Type != TypeCreateRoom &&
		msg.Type != TypeCheckRoom {
		return nil, errors.New("invalid message type")
	}

	// 读取时间戳
	if err := binary.Read(reader, binary.BigEndian, &msg.Timestamp); err != nil {
		return nil, err
	}

	// 读取用户ID
	if err := binary.Read(reader, binary.BigEndian, &msg.UserID); err != nil {
		return nil, err
	}

	// 读取直播间ID
	if err := binary.Read(reader, binary.BigEndian, &msg.LiveID); err != nil {
		return nil, err
	}

	// 读取用户名
	var usernameLen uint16
	if err := binary.Read(reader, binary.BigEndian, &usernameLen); err != nil {
		return nil, err
	}
	usernameBytes := make([]byte, usernameLen)
	if err := binary.Read(reader, binary.BigEndian, &usernameBytes); err != nil {
		return nil, err
	}
	msg.UserName = string(usernameBytes)

	// 读取内容
	var contentLen uint16
	if err := binary.Read(reader, binary.BigEndian, &contentLen); err != nil {
		return nil, err
	}
	contentBytes := make([]byte, contentLen)
	if err := binary.Read(reader, binary.BigEndian, &contentBytes); err != nil {
		return nil, err
	}
	msg.Content = string(contentBytes)

	// 读取颜色
	var colorLen uint16
	if err := binary.Read(reader, binary.BigEndian, &colorLen); err != nil {
		return nil, err
	}
	colorBytes := make([]byte, colorLen)
	if err := binary.Read(reader, binary.BigEndian, &colorBytes); err != nil {
		return nil, err
	}
	msg.Color = string(colorBytes)

	return msg, nil
}

// NewBulletMessage 创建弹幕消息
func NewBulletMessage(liveID, userID uint64, username, content, color string) *BulletMessage {
	if color == "" {
		color = "white" // 默认颜色
	}
	return &BulletMessage{
		Magic:     MagicNumber,
		Version:   CurrentVersion,
		Type:      TypeBullet,
		Timestamp: time.Now().UnixMilli(),
		UserID:    userID,
		LiveID:    liveID,
		UserName:  username,
		Content:   content,
		Color:     color,
	}
}

// NewHeartbeatMessage 创建心跳消息
func NewHeartbeatMessage(userID uint64) *BulletMessage {
	return &BulletMessage{
		Magic:     MagicNumber,
		Version:   CurrentVersion,
		Type:      TypeHeartbeat,
		Timestamp: time.Now().UnixMilli(),
		UserID:    userID,
		UserName:  "",
		Content:   "",
		Color:     "",
	}
}

// NewCreateRoomMessage 创建直播间消息
func NewCreateRoomMessage(liveID, userID uint64) *BulletMessage {
	return &BulletMessage{
		Magic:     MagicNumber,
		Version:   CurrentVersion,
		Type:      TypeCreateRoom,
		Timestamp: time.Now().UnixMilli(),
		UserID:    userID,
		LiveID:    liveID,
		UserName:  "",
		Content:   "",
		Color:     "",
	}
}

// NewCheckRoomMessage 创建检查房间消息
func NewCheckRoomMessage(liveID, userID uint64) *BulletMessage {
	return &BulletMessage{
		Magic:     MagicNumber,
		Version:   CurrentVersion,
		Type:      TypeCheckRoom,
		Timestamp: time.Now().UnixMilli(),
		UserID:    userID,
		LiveID:    liveID,
		UserName:  "",
		Content:   "",
		Color:     "",
	}
}
