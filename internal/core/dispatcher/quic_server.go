package dispatcher

import (
	"context"
	"time"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/quic-go/quic-go"
	"go.uber.org/zap"
)

// QuicServer 管理QUIC相关的连接和操作
type QuicServer struct {
	listener *quic.Listener
	addr     string
}

// NewQuicServer 创建一个新的QUIC服务器
func NewQuicServer(addr, certFile, keyFile string) (*QuicServer, error) {
	// 生成TLS配置
	tlsConfig := util.GenerateTLSConfig(certFile, keyFile)

	// 创建QUIC监听器
	listener, err := quic.ListenAddr(addr, tlsConfig, nil)
	if err != nil {
		return nil, err
	}

	return &QuicServer{
		listener: listener,
		addr:     addr,
	}, nil
}

// Start 启动QUIC服务器并接受连接
func (s *QuicServer) Start(ctx context.Context, connectionHandler func(quic.Connection)) {
	for {
		select {
		case <-ctx.Done():
			logger.Info("QUIC server context canceled, stopping")
			return
		default:
			// 接受新连接
			conn, err := s.listener.Accept(ctx)
			if err != nil {
				logger.Error("Failed to accept QUIC connection", zap.Error(err))
				ob.QUICConnectionErrors.WithLabelValues("dispatcher", "accept").Inc()
				time.Sleep(time.Second) // 避免过快循环
				continue
			}

			// 记录指标
			ob.ActiveQUICConnections.WithLabelValues("dispatcher").Inc()

			// 处理连接（在新的goroutine中）
			go connectionHandler(conn)
		}
	}
}

// AcceptStream 从QUIC连接接受流
func (s *QuicServer) AcceptStream(ctx context.Context, conn quic.Connection) (quic.Stream, error) {
	ctx, span := ob.StartSpan(ctx, "QuicServer.AcceptStream")
	defer span.End()

	startTime := time.Now()
	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		ob.RecordError(span, err, "QuicServer.AcceptStream")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "open_stream").Inc()
		return nil, err
	}

	ob.RecordLatency(ctx, "quic.AcceptStream", time.Since(startTime))
	return stream, nil
}

// ReadInitialMessage 读取初始消息并解码
func (s *QuicServer) ReadInitialMessage(ctx context.Context, stream quic.Stream) (*protocol.BulletMessage, error) {
	// 读取原始数据
	data, err := s.ReadMessage(stream)
	if err != nil {
		return nil, err
	}

	// 解码消息
	ctx, span := ob.StartSpan(ctx, "QuicServer.DecodeMessage")
	defer span.End()

	msg, err := protocol.Decode(data)
	if err != nil {
		ob.RecordError(span, err, "QuicServer.DecodeMessage")
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return nil, err
	}

	return msg, nil
}

// ReadMessage 从QUIC流读取原始消息
func (s *QuicServer) ReadMessage(stream quic.Stream) ([]byte, error) {
	ctx, span := ob.StartSpan(context.Background(), "QuicServer.ReadMessage")
	defer span.End()

	startTime := time.Now()
	buf := make([]byte, 1024)
	n, err := stream.Read(buf)
	if err != nil {
		ob.RecordError(span, err, "QuicServer.ReadMessage")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "read").Inc()
		return nil, err
	}

	ob.RecordLatency(ctx, "quic.Read", time.Since(startTime))
	return buf[:n], nil
}

// WriteToStream 向QUIC流写入数据
func (s *QuicServer) WriteToStream(ctx context.Context, stream quic.Stream, data []byte) error {
	ctx, span := ob.StartSpan(ctx, "QuicServer.WriteToStream")
	defer span.End()

	startTime := time.Now()
	_, err := stream.Write(data)
	if err != nil {
		ob.RecordError(span, err, "QuicServer.WriteToStream")
		ob.QUICConnectionErrors.WithLabelValues("dispatcher", "write").Inc()
		return err
	}

	ob.RecordLatency(ctx, "quic.Write", time.Since(startTime))
	return nil
}

// Close 关闭QUIC服务器
func (s *QuicServer) Close() error {
	return s.listener.Close()
}
