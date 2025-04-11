package websocket

import (
	"context"
	"time"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// RoomManager 管理直播房间
type RoomManager struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewRoomManager 创建新的房间管理器
func NewRoomManager(redisClient *redis.ClusterClient, keyBuilder *pkgcache.RedisKeyBuilder) *RoomManager {
	return &RoomManager{
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
	}
}

// IsRoomExists 检查房间是否存在
func (m *RoomManager) IsRoomExists(ctx context.Context, liveID uint64) (bool, bool) {
	ctx, span := ob.StartSpan(ctx, "gateway.isLiveRoomExists",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	if BackDoorLiveRoomID == liveID {
		return true, true
	}

	key := m.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	exists, err := m.redisClient.SIsMember(ctx, key, liveID).Result()
	if err != nil {
		logger.Error("Failed to check live room existence", zap.Uint64("liveID", liveID), zap.Error(err))
		ob.RecordError(span, err, "gateway.isLiveRoomExists")
		ob.RedisOperations.WithLabelValues("sismember", "failed").Inc()
		return true, false
	}
	ob.RecordLatency(ctx, "redis.SIsMember", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("sismember", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("sismember").Observe(time.Since(startTime).Seconds())
	return exists, false
}

// RegisterRoom 注册直播房间
func (m *RoomManager) RegisterRoom(ctx context.Context, liveID uint64) error {
	ctx, span := ob.StartSpan(ctx, "gateway.registerLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	key := m.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	err := m.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		ob.RecordError(span, err, "gateway.registerLiveRoom")
		ob.RedisOperations.WithLabelValues("sadd", "failed").Inc()
		return err
	}
	err = m.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		ob.RecordError(span, err, "gateway.registerLiveRoom")
		ob.RedisOperations.WithLabelValues("expire", "failed").Inc()
		return err
	}
	ob.RecordLatency(ctx, "redis.SAddAndExpire", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("sadd", "success").Inc()
	ob.RedisOperations.WithLabelValues("expire", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("sadd").Observe(time.Since(startTime).Seconds())
	logger.Info("Live room registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

// UnregisterRoom 注销直播房间
func (m *RoomManager) UnregisterRoom(ctx context.Context, liveID uint64) {
	ctx, span := ob.StartSpan(ctx, "gateway.unregisterLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	key := m.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	err := m.redisClient.SRem(ctx, key, liveID).Err()
	if err != nil {
		logger.Error("Failed to unregister live room",
			zap.Uint64("liveID", liveID),
			zap.Error(err))
		ob.RecordError(span, err, "gateway.unregisterLiveRoom")
		ob.RedisOperations.WithLabelValues("srem", "failed").Inc()
		return
	}
	ob.RecordLatency(ctx, "redis.SRem", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("srem", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("srem").Observe(time.Since(startTime).Seconds())
	logger.Info("Live room unregistered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
}
