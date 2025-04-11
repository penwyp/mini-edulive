package main

import (
	"context"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/dispatcher"
	"github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	// Initialize configuration
	configMgr := config.InitConfig("config/config_dispatcher.yaml")
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
	logger.Info("Starting edulive-dispatcher",
		zap.String("port", cfg.App.Port),
		zap.Strings("redis_addrs", cfg.Redis.Addrs),
		zap.String("quic_addr", cfg.Distributor.QUIC.Addr),
	)

	observability.InitTracing(cfg)

	observability.TryEnablePrometheusExport(cfg)

	// Listen for configuration updates
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	ctx := context.Background()

	// Start Dispatcher
	d, err := dispatcher.NewDispatcher(ctx, cfg)
	if err != nil {
		logger.Panic("Failed to create dispatcher", zap.Error(err))
	}
	d.Start(ctx)
}
