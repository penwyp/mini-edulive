// pkg/kafka/kafka.go
package kafka

import (
	"fmt"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// NewWriter 创建并返回 Kafka Writer
func NewWriter(cfg *config.Kafka) *kafka.Writer {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...), // 使用配置中的 Brokers
		Topic:    cfg.Topic,                 // 使用配置中的 Topic
		Balancer: getBalancer(cfg.Balancer), // 根据配置选择分区策略
	}
	logger.Info("Kafka writer initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic),
		zap.String("balancer", cfg.Balancer))
	return writer
}

// NewReader 创建并返回 Kafka Reader
func NewReader(cfg *config.Kafka) *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID, // 用于消费者组负载均衡
		MinBytes: 10e3,        // 10KB
		MaxBytes: 10e6,        // 10MB
	})
	logger.Info("Kafka reader initialized",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.Topic),
		zap.String("groupID", cfg.GroupID))
	return reader
}

// Dial 用于检查 Kafka 连接状态或获取分区信息
func Dial(broker string) (*kafka.Conn, error) {
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		logger.Error("Failed to connect to Kafka", zap.Error(err))
		return nil, fmt.Errorf("kafka dial failed: %w", err)
	}
	return conn, nil
}

// getBalancer 根据配置返回分区策略
func getBalancer(balanceMethod string) kafka.Balancer {
	switch balanceMethod {
	case "hash":
		return &kafka.Hash{}
	case "referencehash":
		return &kafka.ReferenceHash{}
	case "roundrobin":
		return &kafka.RoundRobin{}
	case "murmur2":
		return &kafka.Murmur2Balancer{}
	case "crc32":
		return &kafka.CRC32Balancer{}
	default:
		return &kafka.Hash{} // 默认使用 Hash 分区
	}
}
