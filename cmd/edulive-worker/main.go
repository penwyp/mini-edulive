package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/worker"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

func main() {
	// Initialize configuration
	configMgr := config.InitConfig("config/config_worker.yaml")
	cfg := configMgr.GetConfig()

	// Initialize logger
	logger.Init(config.Logger{
		Level:      cfg.Logger.Level,
		FilePath:   cfg.Logger.FilePath,
		MaxSize:    cfg.Logger.MaxSize,
		MaxBackups: cfg.Logger.MaxBackups,
		MaxAge:     cfg.Logger.MaxAge,
		Compress:   cfg.Logger.Compress,
	})
	logger.Info("Starting edulive worker",
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.String("kafka_topic", cfg.Kafka.Topic),
		zap.String("redis_addrs", fmt.Sprintf("%v", cfg.Redis.Addrs)),
	)

	// Check Kafka topic message count before starting worker
	checkKafkaMessageCount(cfg)

	// Listen for config updates
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	// Initialize worker
	w, err := worker.NewWorker(cfg)
	if err != nil {
		logger.Panic("Failed to create worker", zap.Error(err))
	}

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// Start worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.Start(ctx)
	}()

	// Wait for termination signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutdown signal received, stopping worker...")

	// Cleanup
	cancel()
	w.Close()
	wg.Wait()
	logger.Sync()
	logger.Info("Worker stopped gracefully")
}

// checkKafkaMessageCount queries and logs the total number of messages in the Kafka topic
func checkKafkaMessageCount(cfg *config.Config) {
	// Kafka connection
	conn, err := pkgkafka.Dial(cfg.Kafka.Brokers[0])
	if err != nil {
		logger.Error("Failed to connect to Kafka for offset check", zap.Error(err))
		return
	}
	defer conn.Close()

	// Get partitions
	partitions, err := conn.ReadPartitions(cfg.Kafka.Topic)
	if err != nil {
		logger.Error("Failed to read partitions", zap.Error(err))
		return
	}

	totalMessages := int64(0)
	for _, p := range partitions {
		// Create a reader for each partition
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   cfg.Kafka.Brokers,
			Topic:     cfg.Kafka.Topic,
			Partition: p.ID,
		})
		defer reader.Close()

		// Get earliest offset
		earliest, err := reader.ReadLag(context.Background())
		if err != nil {
			logger.Error("Failed to get earliest offset",
				zap.Int("partition", p.ID),
				zap.Error(err))
			continue
		}

		// Get latest offset
		reader.SetOffset(kafka.LastOffset)
		latest, err := reader.ReadLag(context.Background())
		if err != nil {
			logger.Error("Failed to get latest offset",
				zap.Int("partition", p.ID),
				zap.Error(err))
			continue
		}

		partitionMessages := latest - earliest
		totalMessages += partitionMessages
		logger.Info("Partition offset info",
			zap.Int("partition", p.ID),
			zap.Int64("earliest_offset", earliest),
			zap.Int64("latest_offset", latest),
			zap.Int64("messages", partitionMessages))
	}

	logger.Info("Total messages in topic",
		zap.String("topic", cfg.Kafka.Topic),
		zap.Int64("total_messages", totalMessages))
	fmt.Printf("Total messages in %s: %d\n", cfg.Kafka.Topic, totalMessages)
}
