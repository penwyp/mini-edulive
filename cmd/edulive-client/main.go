package main

import (
	"context"
	"github.com/penwyp/mini-edulive/client/connection"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	configMgr := config.InitConfig("config/client_config.yaml")
	cfg := configMgr.GetConfig()

	logger.Info("Starting edulive client",
		zap.String("port", cfg.Server.Port),
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
	client, err := connection.NewClient(configMgr.GetConfig())
	if err != nil {
		logger.Panic("Failed to create WebSocket client", zap.Error(err))
	}
	defer client.Close()

	go client.StartHeartbeat()
	go client.Receive(context.Background())

	if err := client.SendBullet("Hello, world!"); err != nil {
		panic(err)
	}
	select {}
}
