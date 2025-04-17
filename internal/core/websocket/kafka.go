// Package websocket 提供了与Kafka的集成功能
package websocket

import (
	"context"
	"encoding/binary"
	"time"

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

// sendToKafka 将消息发送到Kafka
// sendToKafka sends a message to Kafka
func (s *Server) sendToKafka(ctx context.Context, msg *protocol.BulletMessage) error {
	ctx, span := ob.StartSpan(ctx, "gateway.sendToKafka",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.Int64("live_id", int64(msg.LiveID)),
		))
	defer span.End()

	startTime := time.Now()

	// 编码消息
	data, err := msg.Encode(s.config.Performance.BulletCompression)
	if err != nil {
		ob.RecordError(span, err, "gateway.sendToKafka")
		ob.KafkaMessagesSent.WithLabelValues("failed").Inc()
		ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return err
	}

	// 从对象池获取Kafka key
	key := kafkaKeyPool.Get()
	defer kafkaKeyPool.Put(key)

	// 将直播间ID作为Kafka消息的key
	binary.BigEndian.PutUint64(key, msg.LiveID)

	// 写入Kafka
	err = s.kafka.WriteMessages(ctx,
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
