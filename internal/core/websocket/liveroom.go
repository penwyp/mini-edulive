// Package websocket 提供了WebSocket直播间管理功能
package websocket

import (
	"context"
	"time"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// isLiveRoomExists 检查直播间是否存在
func (s *Server) isLiveRoomExists(ctx context.Context, liveID uint64) (bool, bool) {
	ctx, span := ob.StartSpan(ctx, "gateway.isLiveRoomExists",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	// 检查是否为后门直播间ID
	if BackDoorLiveRoomID == liveID {
		return true, true
	}

	// 检查Redis中直播间是否存在
	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	exists, err := s.redisClient.SIsMember(ctx, key, liveID).Result()
	if err != nil {
		logger.Error("Redis operation failed while checking live room existence",
			zap.Uint64("liveID", liveID), zap.Error(err))
		ob.RecordError(span, err, "gateway.isLiveRoomExists")
		ob.RedisOperations.WithLabelValues("sismember", "failed").Inc()
		return true, false
	}

	ob.RecordLatency(ctx, "redis.SIsMember", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("sismember", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("sismember").Observe(time.Since(startTime).Seconds())
	return exists, false
}

// registerLiveRoom 注册一个新的直播间
func (s *Server) registerLiveRoom(ctx context.Context, liveID uint64) error {
	ctx, span := ob.StartSpan(ctx, "gateway.registerLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	// 将直播间ID添加到Redis集合中
	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()

	err := s.redisClient.SAdd(ctx, key, liveID).Err()
	if err != nil {
		ob.RecordError(span, err, "gateway.registerLiveRoom")
		ob.RedisOperations.WithLabelValues("sadd", "failed").Inc()
		return err
	}

	// 设置过期时间以便自动清理
	err = s.redisClient.Expire(ctx, key, 24*time.Hour).Err()
	if err != nil {
		ob.RecordError(span, err, "gateway.registerLiveRoom")
		ob.RedisOperations.WithLabelValues("expire", "failed").Inc()
		return err
	}

	ob.RecordLatency(ctx, "redis.SAddAndExpire", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("sadd", "success").Inc()
	ob.RedisOperations.WithLabelValues("expire", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("sadd").Observe(time.Since(startTime).Seconds())

	logger.Info("Live room successfully registered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
	return nil
}

// unregisterLiveRoom 注销一个直播间
func (s *Server) unregisterLiveRoom(ctx context.Context, liveID uint64) {
	ctx, span := ob.StartSpan(ctx, "gateway.unregisterLiveRoom",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(liveID)),
		))
	defer span.End()

	// 从Redis集合中移除直播间ID
	key := s.keyBuilder.ActiveLiveRoomsKey()
	startTime := time.Now()
	err := s.redisClient.SRem(ctx, key, liveID).Err()
	if err != nil {
		logger.Error("Redis operation failed while unregistering live room",
			zap.Uint64("liveID", liveID),
			zap.Error(err))
		ob.RecordError(span, err, "gateway.unregisterLiveRoom")
		ob.RedisOperations.WithLabelValues("srem", "failed").Inc()
		return
	}

	ob.RecordLatency(ctx, "redis.SRem", time.Since(startTime))
	ob.RedisOperations.WithLabelValues("srem", "success").Inc()
	ob.RedisOperationLatency.WithLabelValues("srem").Observe(time.Since(startTime).Seconds())

	logger.Info("Live room successfully unregistered",
		zap.Uint64("liveID", liveID),
		zap.String("redis_key", key))
}
