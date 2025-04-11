package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"
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

// BulletMessage 定义二进制协议结构体
//
//msgp:tuple BulletMessage
type BulletMessage struct {
	Magic      uint16 `msgp:"magic"`       // 魔数
	Version    uint8  `msgp:"version"`     // 版本号
	Type       uint8  `msgp:"type"`        // 消息类型
	Timestamp  int64  `msgp:"timestamp"`   // 时间戳
	UserID     uint64 `msgp:"user_id"`     // 用户ID
	LiveID     uint64 `msgp:"live_id"`     // 直播间ID
	Username   string `msgp:"username"`    // 用户名（新增）
	ContentLen uint16 `msgp:"content_len"` // 内容长度
	Content    string `msgp:"content"`     // 内容
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
	if err := binary.Write(buf, binary.BigEndian, msg.LiveID); err != nil {
		return nil, err
	}

	// 写入 Username（变长字段，前缀长度 + 内容）
	usernameBytes := []byte(msg.Username)
	usernameLen := uint16(len(usernameBytes))
	if err := binary.Write(buf, binary.BigEndian, usernameLen); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, usernameBytes); err != nil {
		return nil, err
	}

	// 写入 Content
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
	if len(data) < 24 { // 最小长度：魔数(2) + 版本(1) + 类型(1) + 时间戳(8) + 用户ID(8) + 直播间ID(8) + 用户名长度(2)
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

	// 读取用户名长度和内容（新增）
	var usernameLen uint16
	if err := binary.Read(reader, binary.BigEndian, &usernameLen); err != nil {
		return nil, err
	}
	usernameBytes := make([]byte, usernameLen)
	if err := binary.Read(reader, binary.BigEndian, &usernameBytes); err != nil {
		return nil, err
	}
	msg.Username = string(usernameBytes)

	// 读取内容长度和内容
	if err := binary.Read(reader, binary.BigEndian, &msg.ContentLen); err != nil {
		return nil, err
	}
	content := make([]byte, msg.ContentLen)
	if err := binary.Read(reader, binary.BigEndian, &content); err != nil {
		return nil, err
	}
	msg.Content = string(content)

	return msg, nil
}

// NewBulletMessage 创建弹幕消息
func NewBulletMessage(liveID, userID uint64, username, content string) *BulletMessage {
	return &BulletMessage{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Type:       TypeBullet,
		Timestamp:  time.Now().UnixMilli(),
		UserID:     userID,
		LiveID:     liveID,
		Username:   username,
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

// NewCreateRoomMessage 创建直播间消息
func NewCreateRoomMessage(liveID, userID uint64) *BulletMessage {
	return &BulletMessage{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Type:       TypeCreateRoom,
		Timestamp:  time.Now().UnixMilli(),
		UserID:     userID,
		LiveID:     liveID,
		ContentLen: 0,
		Content:    "",
	}
}

// NewCheckRoomMessage 创建检查房间消息
func NewCheckRoomMessage(liveID, userID uint64) *BulletMessage {
	return &BulletMessage{
		Magic:      MagicNumber,
		Version:    CurrentVersion,
		Type:       TypeCheckRoom,
		Timestamp:  time.Now().UnixMilli(),
		UserID:     userID,
		LiveID:     liveID,
		ContentLen: 0,
		Content:    "",
	}
}
