package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"
)

const (
	MagicNumber    = 0xABCD
	CurrentVersion = 0x01
	TypeBullet     = 0x01 // 弹幕消息
	TypeHeartbeat  = 0x02 // 心跳消息
)

// BulletMessage 定义二进制协议结构体
type BulletMessage struct {
	Magic      uint16 // 魔数，固定值 0xABCD
	Version    uint8  // 版本号，当前为 0x01
	Type       uint8  // 消息类型（0x01: 弹幕, 0x02: 心跳）
	Timestamp  int64  // 时间戳（Unix 毫秒）
	UserID     uint64 // 用户ID
	ChannelID  uint64 // 直播间ID
	ContentLen uint16 // 内容长度
	Content    string // 内容（UTF-8 编码）
}

// Encode 将 BulletMessage 编码为二进制数据
func (msg *BulletMessage) Encode() ([]byte, error) {
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
	if err := binary.Write(buf, binary.BigEndian, msg.ChannelID); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, msg.ContentLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, []byte(msg.Content)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Decode 从二进制数据解码为 BulletMessage
func Decode(data []byte) (*BulletMessage, error) {
	if len(data) < 22 { // 最小长度：魔数(2) + 版本(1) + 类型(1) + 时间戳(8) + 用户ID(8) + 内容长度(2)
		return nil, errors.New("data too short")
	}

	reader := bytes.NewReader(data)
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
	if msg.Type != TypeBullet && msg.Type != TypeHeartbeat {
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
	if err := binary.Read(reader, binary.BigEndian, &msg.ChannelID); err != nil {
		return nil, err
	}

	// 读取内容长度
	if err := binary.Read(reader, binary.BigEndian, &msg.ContentLen); err != nil {
		return nil, err
	}

	// 读取内容
	content := make([]byte, msg.ContentLen)
	if err := binary.Read(reader, binary.BigEndian, &content); err != nil {
		return nil, err
	}
	msg.Content = string(content)

	return msg, nil
}

// NewBulletMessage 创建弹幕消息
func NewBulletMessage(channelId, userID uint64, content string) *BulletMessage {
	return &BulletMessage{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Type:       TypeBullet,
		Timestamp:  time.Now().UnixMilli(),
		UserID:     userID,
		ChannelID:  channelId,
		ContentLen: uint16(len(content)),
		Content:    content,
	}
}

// NewHeartbeatMessage 创建心跳消息
func NewHeartbeatMessage(userID uint64) *BulletMessage {
	return &BulletMessage{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Type:       TypeHeartbeat,
		Timestamp:  time.Now().UnixMilli(),
		UserID:     userID,
		ContentLen: 0,
		Content:    "",
	}
}
