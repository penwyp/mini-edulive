// cmd/edulive-client/main.go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/penwyp/mini-edulive/client/connection"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func main() {
	// 初始化配置
	configMgr := config.InitConfig("config/config_client.yaml")
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
	logger.Info("Starting edulive client",
		zap.String("websocket_endpoint", cfg.WebSocket.Endpoint),
		zap.String("quic_addr", cfg.Distributor.QUIC.Addr),
		zap.Uint64("liveID", cfg.Client.LiveID),
		zap.Uint64("userID", cfg.Client.UserID),
		zap.String("mode", cfg.Client.Mode))

	// 监听配置变更
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	// 创建 WebSocket 客户端（发送弹幕或创建直播间）
	wsClient, err := connection.NewClient(cfg)
	if err != nil {
		logger.Panic("Failed to create WebSocket client", zap.Error(err))
	}
	defer wsClient.Close()

	// 创建 QUIC 客户端（接收弹幕，适用于 create 和 send 模式）
	quicClient, err := connection.NewQuicClient(cfg)
	if err != nil {
		logger.Panic("Failed to create QUIC client", zap.Error(err))
	}
	defer quicClient.Close()

	// 启动心跳和接收线程
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// 启动 WebSocket 心跳
	wg.Add(1)
	go func() {
		defer wg.Done()
		wsClient.StartHeartbeat(ctx)
	}()

	// 启动 QUIC 接收（适用于所有模式）
	wg.Add(1)
	go func() {
		defer wg.Done()
		quicClient.Receive(ctx)
	}()

	// 根据模式执行逻辑
	if cfg.Client.Mode == "create" {
		// 创建直播间模式
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := wsClient.CreateRoom(cfg.Client.LiveID, cfg.Client.UserID)
			if err != nil {
				logger.Error("Failed to create room", zap.Error(err))
			} else {
				logger.Info("Room created successfully",
					zap.Uint64("liveID", cfg.Client.LiveID),
					zap.Uint64("userID", cfg.Client.UserID))
			}
		}()
	} else if cfg.Client.Mode == "send" {
		// 发送弹幕模式
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(cfg.Client.SendInterval)
			defer ticker.Stop()
			count := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					count++
					content := fmt.Sprintf("Bullet %d from user %d", count, cfg.Client.UserID)
					if err := wsClient.SendBullet(content); err != nil {
						logger.Warn("Failed to send bullet", zap.Error(err))
					}
				}
			}
		}()
	} else {
		logger.Panic("Unsupported client mode", zap.String("mode", cfg.Client.Mode))
	}

	// 等待终止信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutdown signal received, stopping client...")

	// 清理
	cancel()
	wg.Wait()
	logger.Sync()
	logger.Info("Client stopped gracefully")
}
