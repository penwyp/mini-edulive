package main

import (
	"context"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/internal/core/websocket"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	configMgr := config.InitConfig("config/config_gateway.yaml")
	cfg := configMgr.GetConfig()

	logger.Init(config.Logger{
		Level:      cfg.Logger.Level,
		FilePath:   cfg.Logger.FilePath,
		MaxSize:    cfg.Logger.MaxSize,
		MaxBackups: cfg.Logger.MaxBackups,
		MaxAge:     cfg.Logger.MaxAge,
		Compress:   cfg.Logger.Compress,
	})
	logger.Info("Starting edulive gateway",
		zap.String("port", cfg.App.Port),
		zap.Bool("websocket_enabled", cfg.WebSocket.Enabled),
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
	)

	observability.InitTracing(cfg)

	// Listen for configuration updates
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	observability.TryEnablePrometheusExport(cfg)

	// Start system
	gateway := websocket.NewServer(configMgr.GetConfig())
	gateway.Start(context.Background())
}
