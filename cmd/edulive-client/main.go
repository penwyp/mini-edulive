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
		zap.String("userName", cfg.Client.UserName),
		zap.String("mode", cfg.Client.Mode))

	// 监听配置更新
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	// 初始化 WebSocket 和 QUIC 客户端
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

	// 启动 WebSocket 心跳，并支持重连
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryInterval := time.Duration(cfg.Client.SendInterval)
		maxRetries := cfg.Client.MaxRetries
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := wsClient.StartHeartbeat(ctx)
				if err != nil {
					logger.Warn("WebSocket heartbeat failed, attempting to reconnect",
						zap.Error(err),
						zap.Int("retry_interval_ms", int(retryInterval.Milliseconds())))
					if maxRetries == 0 || attemptReconnect(ctx, wsClient, cfg, maxRetries, retryInterval) {
						continue
					}
					logger.Error("Max retries reached for WebSocket, stopping reconnection")
					cancel()
					return
				}
			}
		}
	}()

	// 启动 QUIC 接收，并支持重连
	wg.Add(1)
	go func() {
		defer wg.Done()
		retryInterval := time.Duration(cfg.Client.SendInterval)
		maxRetries := cfg.Client.MaxRetries
		for {
			select {
			case <-ctx.Done():
				return
			default:
				err := quicClient.Receive(ctx)
				if err != nil {
					logger.Warn("QUIC receive failed, attempting to reconnect",
						zap.Error(err),
						zap.Int("retry_interval_ms", int(retryInterval.Milliseconds())))
					if maxRetries == 0 || attemptQuicReconnect(ctx, quicClient, cfg, maxRetries, retryInterval) {
						continue
					}
					logger.Error("Max retries reached for QUIC, stopping reconnection")
					cancel()
					return
				}
			}
		}
	}()

	// 处理客户端模式
	if cfg.Client.Mode == "create" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := wsClient.CreateRoom(cfg.Client.LiveID, cfg.Client.UserID, cfg.Client.UserName)
			if err != nil {
				logger.Error("Failed to create room", zap.Error(err))
				os.Exit(1)
			}
			logger.Info("Room created successfully",
				zap.Uint64("liveID", cfg.Client.LiveID),
				zap.Uint64("userID", cfg.Client.UserID),
				zap.String("userName", cfg.Client.UserName),
			)
		}()
	} else if cfg.Client.Mode == "send" {
		// 检查房间是否存在
		logger.Info("Checking room existence...", zap.Uint64("liveID", cfg.Client.LiveID))
		err := wsClient.CheckRoom(cfg.Client.LiveID, cfg.Client.UserID, cfg.Client.UserName)
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

	// 等待终止信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutdown signal received, stopping client...")

	cancel()
	wg.Wait()
	logger.Sync()
	logger.Info("Client stopped gracefully")
}

// attemptReconnect 尝试重新连接 WebSocket
func attemptReconnect(ctx context.Context, wsClient *connection.Client, cfg *config.Config, maxRetries int, retryInterval time.Duration) bool {
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return false
		default:
			logger.Info("Attempting WebSocket reconnect", zap.Int("attempt", i+1))
			newWsClient, err := connection.NewClient(cfg)
			if err == nil {
				wsClient.Close()
				*wsClient = *newWsClient
				logger.Info("WebSocket reconnected successfully")
				return true
			}
			logger.Warn("WebSocket reconnect attempt failed", zap.Error(err))
			time.Sleep(retryInterval)
		}
	}
	return false
}

// attemptQuicReconnect 尝试重新连接 QUIC
func attemptQuicReconnect(ctx context.Context, quicClient *connection.QuicClient, cfg *config.Config, maxRetries int, retryInterval time.Duration) bool {
	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return false
		default:
			logger.Info("Attempting QUIC reconnect", zap.Int("attempt", i+1))
			newQuicClient, err := connection.NewQuicClient(cfg)
			if err == nil {
				quicClient.Close()
				*quicClient = *newQuicClient
				logger.Info("QUIC reconnected successfully")
				return true
			}
			logger.Warn("QUIC reconnect attempt failed", zap.Error(err))
			time.Sleep(retryInterval)
		}
	}
	return false
}
