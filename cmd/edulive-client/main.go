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
	configMgr := config.InitConfig("config/config_client.yaml")
	cfg := configMgr.GetConfig()

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

	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	wsClient, err := connection.NewClient(cfg)
	if err != nil {
		logger.Panic("Failed to create WebSocket client", zap.Error(err))
	}
	defer wsClient.Close()

	quicClient, err := connection.NewQuicClient(cfg)
	if err != nil {
		logger.Panic("Failed to create QUIC client", zap.Error(err))
	}
	defer quicClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		wsClient.StartHeartbeat(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		quicClient.Receive(ctx)
	}()

	if cfg.Client.Mode == "create" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := wsClient.CreateRoom(cfg.Client.LiveID, cfg.Client.UserID)
			if err != nil {
				logger.Error("Failed to create room", zap.Error(err))
				os.Exit(1)
			}
			logger.Info("Room created successfully",
				zap.Uint64("liveID", cfg.Client.LiveID),
				zap.Uint64("userID", cfg.Client.UserID))
		}()
	} else if cfg.Client.Mode == "send" {
		// 检查房间是否存在
		logger.Info("Checking room existence...", zap.Uint64("liveID", cfg.Client.LiveID))
		err := wsClient.CheckRoom(cfg.Client.LiveID, cfg.Client.UserID)
		if err != nil {
			logger.Error("Room check failed", zap.Error(err))
			cancel()
			wg.Wait()
			logger.Sync()
			os.Exit(1)
		}

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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutdown signal received, stopping client...")

	cancel()
	wg.Wait()
	logger.Sync()
	logger.Info("Client stopped gracefully")
}
