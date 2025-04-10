package main

import (
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/dispatcher"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	// 初始化配置
	configMgr := config.InitConfig("config/config_dispatcher.yaml")
	cfg := configMgr.GetConfig()

	// 初始化日志
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

	// 监听配置变更
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	// 启动 Dispatcher
	d, err := dispatcher.NewDispatcher(cfg)
	if err != nil {
		logger.Panic("Failed to create dispatcher", zap.Error(err))
	}
	d.Start()
}
