package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/client"
	"github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

// ConnectionState 表示连接状态
type ConnectionState int

const (
	ConnectionInitializing ConnectionState = iota
	ConnectionConnected
	ConnectionDisconnected
	ConnectionFailed
)

// initializeConfig 初始化配置
func initializeConfig(configPath string) (*config.Config, *config.ConfigManager) {
	// 检查配置文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: Configuration file %s does not exist\n", configPath)
		os.Exit(1)
	}

	// 初始化配置
	configMgr := config.InitConfig(configPath)
	return configMgr.GetConfig(), configMgr
}

// setupLogger 设置日志系统
func setupLogger(cfg *config.Config, configPath string) {
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
		zap.String("mode", cfg.Client.Mode),
		zap.String("config_path", configPath))
}

// initializeClients 初始化WebSocket和QUIC客户端
func initializeClients(ctx context.Context, cfg *config.Config) (client.Client, client.Client, error) {
	// 初始化WebSocket客户端
	wsClient, err := client.NewClient(ctx, cfg, client.WebSocketClientType)
	if err != nil {
		logger.Error("Failed to create WebSocket client", zap.Error(err))
		return nil, nil, err
	}

	// 初始化QUIC客户端
	quicClient, err := client.NewClient(ctx, cfg, client.QUICClientType)
	if err != nil {
		logger.Error("Failed to create QUIC client", zap.Error(err))
		wsClient.Close(ctx) // 清理已创建的资源
		return nil, nil, err
	}

	return wsClient, quicClient, nil
}

// startWebSocketHeartbeat 启动WebSocket心跳
func startWebSocketHeartbeat(ctx context.Context, cancel context.CancelFunc,
	wsClient *client.Client, cfg *config.Config, wg *sync.WaitGroup, connState *ConnectionState, connMutex *sync.RWMutex) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryInterval := cfg.Client.SendInterval
		maxRetries := cfg.Client.MaxRetries

		for {
			select {
			case <-ctx.Done():
				return
			default:
				connMutex.RLock()
				currentState := *connState
				connMutex.RUnlock()

				// 如果连接已断开，尝试重连
				if currentState == ConnectionDisconnected {
					logger.Info("Connection is disconnected, attempting to reconnect")
					if maxRetries == 0 || attemptReconnect(ctx, wsClient, cfg, maxRetries, retryInterval, client.WebSocketClientType) {
						connMutex.Lock()
						*connState = ConnectionConnected
						connMutex.Unlock()
						continue
					} else {
						logger.Error("Max retries reached for WebSocket, stopping reconnection")
						connMutex.Lock()
						*connState = ConnectionFailed
						connMutex.Unlock()
						return
					}
				}

				if err := (*wsClient).StartHeartbeat(ctx); err != nil {
					logger.Warn("WebSocket heartbeat failed, marking connection as disconnected",
						zap.Error(err),
						zap.Int("retry_interval_ms", int(retryInterval.Milliseconds())))

					connMutex.Lock()
					*connState = ConnectionDisconnected
					connMutex.Unlock()

					// 立即尝试重连
					if maxRetries == 0 || attemptReconnect(ctx, wsClient, cfg, maxRetries, retryInterval, client.WebSocketClientType) {
						connMutex.Lock()
						*connState = ConnectionConnected
						connMutex.Unlock()
						continue
					} else {
						logger.Error("Max retries reached for WebSocket, stopping reconnection")
						connMutex.Lock()
						*connState = ConnectionFailed
						connMutex.Unlock()
						return
					}
				}
			}
		}
	}()
}

// startQUICReceive 启动QUIC接收
func startQUICReceive(ctx context.Context, cancel context.CancelFunc,
	quicClient *client.Client, cfg *config.Config, wg *sync.WaitGroup) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		retryInterval := cfg.Client.SendInterval
		maxRetries := cfg.Client.MaxRetries

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// 类型断言访问QUIC特定方法
				if quicClientImpl, ok := (*quicClient).(*client.QUICClient); ok {
					if err := quicClientImpl.Receive(ctx); err != nil {
						logger.Warn("QUIC receive failed, attempting to reconnect",
							zap.Error(err),
							zap.Int("retry_interval_ms", int(retryInterval.Milliseconds())))

						if maxRetries == 0 || attemptReconnect(ctx, quicClient, cfg, maxRetries,
							retryInterval, client.QUICClientType) {
							// 重连成功后，需要获取新实例进行类型断言
							if newQuicClientImpl, ok := (*quicClient).(*client.QUICClient); ok {
								quicClientImpl = newQuicClientImpl
							}
							continue
						}

						logger.Error("Max retries reached for QUIC, stopping reconnection")
						// 虽然QUIC失败了，但不影响主程序运行
						return
					}
				} else {
					logger.Error("Failed to type assert QUIC client")
					return
				}
			}
		}
	}()
}

// handleCreateMode 处理创建模式
func handleCreateMode(ctx context.Context, cancel context.CancelFunc,
	wsClient client.Client, cfg *config.Config, wg *sync.WaitGroup, connState *ConnectionState, connMutex *sync.RWMutex) {

	wg.Add(1)
	go func() {
		defer wg.Done()
		// 使用超时上下文
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 10*time.Second)
		defer timeoutCancel()

		// 检查连接状态
		connMutex.RLock()
		currentState := *connState
		connMutex.RUnlock()

		if currentState != ConnectionConnected {
			logger.Warn("Connection is not ready, waiting...", zap.Int("state", int(currentState)))
			// 等待连接恢复，最多等待10秒
			for i := 0; i < 20; i++ {
				time.Sleep(500 * time.Millisecond)
				connMutex.RLock()
				currentState = *connState
				connMutex.RUnlock()
				if currentState == ConnectionConnected {
					break
				}
			}
		}

		if currentState != ConnectionConnected {
			logger.Error("Connection is still not ready, but trying to create room anyway")
		}

		err := wsClient.CreateRoom(timeoutCtx, cfg.Client.LiveID, cfg.Client.UserID, cfg.Client.UserName)
		if err != nil {
			logger.Error("Failed to create room", zap.Error(err))
			logger.Warn("Continuing despite room creation failure")
			return
		}
		logger.Info("Room created successfully",
			zap.Uint64("liveID", cfg.Client.LiveID),
			zap.Uint64("userID", cfg.Client.UserID),
			zap.String("userName", cfg.Client.UserName),
		)
	}()
}

// handleSendMode 处理发送模式
func handleSendMode(ctx context.Context, cancel context.CancelFunc,
	wsClient client.Client, cfg *config.Config, wg *sync.WaitGroup, connState *ConnectionState, connMutex *sync.RWMutex) {

	// 检查房间是否存在，使用超时上下文
	logger.Info("Checking room existence...", zap.Uint64("liveID", cfg.Client.LiveID))

	// 创建一个单独的上下文用于CheckRoom操作，设置较短的超时时间
	checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
	defer checkCancel()

	err := wsClient.CheckRoom(checkCtx, cfg.Client.LiveID, cfg.Client.UserID, cfg.Client.UserName)
	if err != nil {
		logger.Warn("Room check failed, attempting to continue anyway", zap.Error(err))
		// 检查失败不影响继续运行，可能是服务器问题
	} else {
		logger.Info("Room check successful", zap.Uint64("liveID", cfg.Client.LiveID))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(cfg.Client.SendInterval)
		defer ticker.Stop()
		count := 0

		// 添加一个短暂的延迟，确保客户端完全连接
		time.Sleep(1 * time.Second)

		var consecutiveFailures int = 0
		maxConsecutiveFailures := 5 // 连续失败5次后等待并重试

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 检查连接状态
				connMutex.RLock()
				currentState := *connState
				connMutex.RUnlock()

				if currentState != ConnectionConnected {
					logger.Warn("Connection not ready for sending, waiting...", zap.Int("state", int(currentState)))
					consecutiveFailures++

					// 如果连续多次失败，等待连接恢复
					if consecutiveFailures >= maxConsecutiveFailures {
						logger.Warn("Too many consecutive send failures, pausing for reconnection",
							zap.Int("failures", consecutiveFailures))
						time.Sleep(3 * time.Second)
						consecutiveFailures = 0
					}
					continue
				}

				count++
				content := fmt.Sprintf("Bullet %d from user %d", count, cfg.Client.UserID)

				// 为Send操作创建超时上下文
				sendCtx, sendCancel := context.WithTimeout(ctx, 2*time.Second)
				err := wsClient.Send(sendCtx, content)
				sendCancel()

				if err != nil {
					logger.Warn("Failed to send bullet", zap.Error(err))

					// 检测是否为连接关闭错误
					if isConnectionClosed(err) {
						logger.Warn("Connection appears to be closed, marking as disconnected")
						connMutex.Lock()
						*connState = ConnectionDisconnected
						connMutex.Unlock()
						consecutiveFailures++
					} else {
						consecutiveFailures++
					}

					// 如果连续多次失败，暂停一段时间
					if consecutiveFailures >= maxConsecutiveFailures {
						logger.Warn("Too many consecutive send failures, pausing for reconnection",
							zap.Int("failures", consecutiveFailures))
						time.Sleep(3 * time.Second)
						consecutiveFailures = 0
					}
				} else {
					// 发送成功，重置失败计数
					consecutiveFailures = 0
				}
			}
		}
	}()
}

// 检测是否为连接关闭错误
func isConnectionClosed(err error) bool {
	errStr := err.Error()
	return errStr == "connection closed" ||
		errStr == "use of closed network connection" ||
		errStr == "websocket: close sent" ||
		errStr == "connection reset by peer" ||
		errStr == "broken pipe" ||
		errStr == "connection error during write: failed to write msg: use of closed network connection"
}

// waitForShutdown 等待关闭信号
func waitForShutdown(cancel context.CancelFunc, wg *sync.WaitGroup) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	logger.Info("Shutdown signal received, stopping client...")
	cancel()
	wg.Wait()
	logger.Sync()
	logger.Info("Client stopped gracefully")
}

func main() {
	// 解析命令行参数
	configPath := flag.String("config", "config/config_client.yaml", "Path to the configuration file")
	flag.Parse()

	// 初始化配置和日志
	cfg, configMgr := initializeConfig(*configPath)
	setupLogger(cfg, *configPath)

	// 初始化可观测性
	observability.InitTracing(cfg)
	observability.TryEnablePrometheusExport(cfg)

	// 监听配置更新
	go func() {
		for newCfg := range configMgr.ConfigChan {
			logger.Info("Configuration updated", zap.Any("new_config", newCfg))
		}
	}()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化连接状态
	connState := ConnectionInitializing
	var connMutex sync.RWMutex

	// 初始化客户端
	wsClient, quicClient, err := initializeClients(ctx, cfg)
	if err != nil {
		logger.Error("Failed to initialize clients, exiting", zap.Error(err))
		os.Exit(1)
	}

	// 设置连接状态为已连接
	connMutex.Lock()
	connState = ConnectionConnected
	connMutex.Unlock()

	// 设置资源清理
	defer func() {
		if wsClient != nil {
			wsClient.Close(ctx)
		}
		if quicClient != nil {
			quicClient.Close(ctx)
		}
	}()

	var wg sync.WaitGroup

	// 启动WebSocket心跳
	wsClientPtr := &wsClient
	startWebSocketHeartbeat(ctx, cancel, wsClientPtr, cfg, &wg, &connState, &connMutex)

	// 启动QUIC接收
	quicClientPtr := &quicClient
	startQUICReceive(ctx, cancel, quicClientPtr, cfg, &wg)

	// 等待客户端完全初始化
	time.Sleep(500 * time.Millisecond)

	// 处理客户端模式
	switch cfg.Client.Mode {
	case "create":
		handleCreateMode(ctx, cancel, wsClient, cfg, &wg, &connState, &connMutex)
	case "send":
		handleSendMode(ctx, cancel, wsClient, cfg, &wg, &connState, &connMutex)
	default:
		logger.Warn("Unsupported client mode, running in passive mode", zap.String("mode", cfg.Client.Mode))
	}

	// 等待关闭信号
	waitForShutdown(cancel, &wg)
}

// attemptReconnect 尝试重新连接
func attemptReconnect(ctx context.Context, clientPtr *client.Client, cfg *config.Config,
	maxRetries int, retryInterval time.Duration, clientType client.ClientType) bool {

	for i := 0; i < maxRetries; i++ {
		select {
		case <-ctx.Done():
			return false
		default:
			logger.Info("Attempting client reconnect",
				zap.Int("clientType", int(clientType)),
				zap.Int("attempt", i+1))

			// 为重连操作设置一个超时上下文
			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			newClient, err := client.NewClient(timeoutCtx, cfg, clientType)
			cancel()

			if err == nil {
				if *clientPtr != nil {
					oldClient := *clientPtr
					oldClient.Close(ctx)
				}
				*clientPtr = newClient
				logger.Info("Client reconnected successfully",
					zap.Int("clientType", int(clientType)))
				return true
			} else {
				logger.Warn("Client reconnect attempt failed",
					zap.Int("clientType", int(clientType)),
					zap.Error(err))

				// 指数退避策略，每次失败后增加等待时间
				backoffTime := retryInterval * time.Duration(i+1)
				if backoffTime > 10*time.Second {
					backoffTime = 10 * time.Second
				}
				time.Sleep(backoffTime)
			}
		}
	}
	return false
}
