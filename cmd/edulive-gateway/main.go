package main

import (
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/websocket"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	configMgr := config.InitConfig("config/config_gateway.yaml")
	cfg := configMgr.GetConfig()

	logger.Info("Starting edulive gateway",
		zap.String("port", cfg.App.Port),
		zap.Bool("websocket_enabled", cfg.WebSocket.Enabled),
		zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
	)

	// 监听配置变更
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	// 启动系统
	gateway := websocket.NewServer(configMgr.GetConfig())
	gateway.Start()
}
