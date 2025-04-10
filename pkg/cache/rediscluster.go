package cache

import (
	"context"
	"fmt"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NewRedisClusterClient 创建并返回 Redis Cluster 客户端
func NewRedisClusterClient(cfg *config.Redis) (*redis.ClusterClient, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    cfg.Addrs,
		Password: cfg.Password,
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		logger.Error("Failed to ping Redis Cluster", zap.Error(err))
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}
	logger.Info("Redis Cluster client initialized",
		zap.Strings("addrs", cfg.Addrs))
	return client, nil
}
