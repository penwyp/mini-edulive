package client

import (
	"errors"
	"fmt"
)

// 客户端错误类型
var (
	ErrConnectionClosed       = errors.New("connection is closed")
	ErrUnsupportedClientType  = errors.New("unsupported client type")
	ErrUnexpectedResponseType = errors.New("unexpected response type")
	ErrRoomDoesNotExist       = errors.New("room does not exist")
	ErrConnectionFailed       = errors.New("connection failed")
	ErrTLSConfigFailed        = errors.New("TLS configuration failed")
)

// ProtocolError 表示协议相关错误
type ProtocolError struct {
	Op  string // 操作
	Err error  // 错误
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error during %s: %v", e.Op, e.Err)
}

func (e *ProtocolError) Unwrap() error {
	return e.Err
}

// ConnectionError 表示连接相关错误
type ConnectionError struct {
	Op  string // 操作
	Err error  // 错误
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection error during %s: %v", e.Op, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// NewProtocolError 创建一个新的协议错误
func NewProtocolError(op string, err error) error {
	return &ProtocolError{Op: op, Err: err}
}

// NewConnectionError 创建一个新的连接错误
func NewConnectionError(op string, err error) error {
	return &ConnectionError{Op: op, Err: err}
}
