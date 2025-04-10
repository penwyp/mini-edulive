package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/worker"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	// Initialize configuration
	configMgr := config.InitConfig("config/config_worker.yaml")
	cfg := configMgr.GetConfig()

	// Initialize logger
	logger.Init(cfg.Logger)
	logger.Info("Starting edulive worker",
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
		zap.String("kafka_topic", cfg.Kafka.Topic),
		zap.String("redis_addr", cfg.Redis.Addr),
	)

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
