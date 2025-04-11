package websocket

import "github.com/penwyp/mini-edulive/pkg/protocol"

// messageTypeToString 将消息类型转换为字符串，便于追踪
func messageTypeToString(t uint8) string {
	switch t {
	case protocol.TypeBullet:
		return "bullet"
	case protocol.TypeHeartbeat:
		return "heartbeat"
	case protocol.TypeCreateRoom:
		return "create_room"
	case protocol.TypeCheckRoom:
		return "check_room"
	default:
		return "unknown"
	}
}
