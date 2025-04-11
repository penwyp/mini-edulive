package websocket

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/penwyp/mini-edulive/config"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Kafka key 字节切片的池
var kafkaKeyPool = pool.RegisterPool("kafka_key", func() []byte {
	return make([]byte, 8)
})

// MessageProcessor 负责处理消息逻辑
type MessageProcessor struct {
	kafka       *kafka.Writer
	roomManager *RoomManager
	config      *config.Config
}

// NewMessageProcessor 创建新的消息处理器
func NewMessageProcessor(kafka *kafka.Writer, roomManager *RoomManager, cfg *config.Config) *MessageProcessor {
	return &MessageProcessor{
		kafka:       kafka,
		roomManager: roomManager,
		config:      cfg,
	}
}

// SendToKafka 发送消息到Kafka
func (p *MessageProcessor) SendToKafka(ctx context.Context, msg *protocol.BulletMessage) error {
	ctx, span := ob.StartSpan(ctx, "gateway.sendToKafka",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer span.End()

	startTime := time.Now()
	data, err := msg.Encode(p.config.GetBulletCompression())
	if err != nil {
		ob.RecordError(span, err, "gateway.sendToKafka")
		ob.KafkaMessagesSent.WithLabelValues("failed").Inc()
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return err
	}

	key := kafkaKeyPool.Get()
	defer kafkaKeyPool.Put(key)

	binary.BigEndian.PutUint64(key, msg.LiveID)

	err = p.kafka.WriteMessages(ctx,
		kafka.Message{
			Key:   key,
			Value: data,
		},
	)
	if err != nil {
		ob.RecordError(span, err, "gateway.sendToKafka")
		ob.KafkaMessagesSent.WithLabelValues("failed").Inc()
		return err
	}
	ob.RecordLatency(ctx, "kafka.WriteMessages", time.Since(startTime))
	ob.KafkaMessagesSent.WithLabelValues("success").Inc()
	ob.KafkaMessageLatency.WithLabelValues().Observe(time.Since(startTime).Seconds())
	return nil
}

// IsRoomExists 检查房间是否存在
func (p *MessageProcessor) IsRoomExists(ctx context.Context, liveID uint64) (bool, bool) {
	return p.roomManager.IsRoomExists(ctx, liveID)
}

// RegisterRoom 注册直播房间
func (p *MessageProcessor) RegisterRoom(ctx context.Context, liveID uint64) error {
	return p.roomManager.RegisterRoom(ctx, liveID)
}

// UnregisterRoom 注销直播房间
func (p *MessageProcessor) UnregisterRoom(ctx context.Context, liveID uint64) {
	p.roomManager.UnregisterRoom(ctx, liveID)
}
