package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// BulletStorage handles persistence of bullet messages
type BulletStorage struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewBulletStorage creates a new bullet storage
func NewBulletStorage(redisClient *redis.ClusterClient, keyBuilder *pkgcache.RedisKeyBuilder) *BulletStorage {
	return &BulletStorage{
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
	}
}

// Store persists a bullet message to Redis
func (s *BulletStorage) Store(spanCtx context.Context, msg *protocol.BulletMessage) error {
	ctx, span := observability.StartSpan(spanCtx, "BulletStorage.Store",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(msg.LiveID)),
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.String("user_name", msg.UserName),
		))
	defer span.End()

	startTime := time.Now()
	pipe := s.redisClient.Pipeline()

	serializedBullet := protocol.SerializedBullet{
		Timestamp: msg.Timestamp,
		UserID:    msg.UserID,
		LiveID:    msg.LiveID,
		UserName:  msg.UserName,
		Content:   msg.Content,
		Color:     msg.Color,
	}

	serializedData, err := json.Marshal(serializedBullet)
	if err != nil {
		logger.Error("Failed to marshal serialized bullet", zap.Error(err))
		observability.RecordError(span, err, "BulletStorage.Store")
		observability.BulletMessagesProcessed.WithLabelValues("worker", "failed").Inc()
		return err
	}

	bulletKey := s.keyBuilder.LiveBulletKey(msg.LiveID)
	uniqueMember := fmt.Sprintf("%d:%s:%s", msg.UserID, util.FormatUint64ToString(uint64(msg.Timestamp)), string(serializedData))
	pipe.ZAdd(ctx, bulletKey, redis.Z{
		Score:  float64(msg.Timestamp),
		Member: uniqueMember,
	})

	// Keep only the most recent 1000 messages
	pipe.ZRemRangeByRank(ctx, bulletKey, 0, -1001)
	logger.Debug("Prepared bullet storage",
		zap.String("bullet_key", bulletKey),
		zap.String("serialized_data", string(serializedData)))

	// Update user ranking for this live
	rankingKey := s.keyBuilder.LiveRankingKey(msg.LiveID)
	pipe.ZIncrBy(ctx, rankingKey, 1, s.keyBuilder.UserIDStr(msg.UserID))
	logger.Debug("Prepared ranking update",
		zap.String("ranking_key", rankingKey),
		zap.String("userID_str", s.keyBuilder.UserIDStr(msg.UserID)))

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		logger.Error("Failed to store message in Redis",
			zap.Uint64("liveID", msg.LiveID),
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName),
			zap.Error(err))
		observability.RecordError(span, err, "BulletStorage.Store")
		observability.RedisOperations.WithLabelValues("pipeline", "failed").Inc()
		observability.RankingUpdates.WithLabelValues("failed").Inc()
		observability.BulletMessagesProcessed.WithLabelValues("worker", "failed").Inc()
		return err
	}

	// Set expiration on the bullet key
	pipe.Expire(ctx, bulletKey, 24*time.Hour)
	observability.RedisOperations.WithLabelValues("pipeline", "success").Inc()
	observability.RankingUpdates.WithLabelValues("success").Inc()
	observability.RecordLatency(ctx, "redis.PipelineExec", time.Since(startTime))
	observability.RedisOperationLatency.WithLabelValues("pipeline").Observe(time.Since(startTime).Seconds())
	logger.Debug("Redis pipeline executed",
		zap.String("bullet_key", bulletKey),
		zap.String("ranking_key", rankingKey),
		zap.Duration("execution_time", time.Since(startTime)))

	logger.Info("Message stored",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName),
		zap.String("content", msg.Content))

	return nil
}
