// internal/core/worker/worker.go
package worker

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability" // 引入可观测性包
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute" // 用于添加 Span 属性
	"go.opentelemetry.io/otel/trace"     // 用于 Span 操作
	"go.uber.org/zap"
)

type Worker struct {
	config      *config.Config
	kafkaReader *kafka.Reader
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
	wg          sync.WaitGroup
}

func NewWorker(spanCtx context.Context, cfg *config.Config) (*Worker, error) {
	// 创建根 Span 追踪 Worker 初始化
	ctx, span := observability.StartSpan(spanCtx, "worker.NewWorker")
	defer span.End()

	startTime := time.Now()
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		observability.RecordError(span, err, "worker.NewWorker")
		return nil, err
	}
	observability.RecordLatency(ctx, "redis.NewClusterClient", time.Since(startTime))

	w := &Worker{
		config:      cfg,
		kafkaReader: pkgkafka.NewReader(&cfg.Kafka),
		redisClient: redisClient,
		keyBuilder:  pkgcache.NewRedisKeyBuilder(),
	}
	return w, nil
}

func (w *Worker) Start(spanCtx context.Context) {
	// 创建根 Span 追踪 Worker 启动
	ctx, span := observability.StartSpan(spanCtx, "worker.Start")
	defer span.End()

	logger.Info("Worker started, consuming Kafka messages...")

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				logger.Info("Worker context canceled, stopping...")
				return
			default:
				// 创建子 Span 追踪单次消息消费
				msgCtx, msgSpan := observability.StartSpan(context.Background(), "worker.consumeMessage")
				startTime := time.Now()

				msg, err := w.kafkaReader.ReadMessage(msgCtx)
				if err != nil {
					logger.Error("Failed to read Kafka message", zap.Error(err))
					observability.RecordError(msgSpan, err, "worker.consumeMessage")
					msgSpan.End()
					continue
				}
				msgSpan.SetAttributes(
					attribute.Int64("offset", msg.Offset),
					attribute.Int("partition", msg.Partition),
					attribute.String("key", string(msg.Key)),
				)
				observability.RecordLatency(msgCtx, "kafka.ReadMessage", time.Since(startTime))
				logger.Debug("Kafka message received",
					zap.Int64("offset", msg.Offset),
					zap.Int("partition", msg.Partition),
					zap.ByteString("key", msg.Key),
					zap.Duration("read_latency", time.Since(startTime)))

				w.processMessage(msgCtx, msg)
				msgSpan.End()
			}
		}
	}()
}

func (w *Worker) processMessage(spanCtx context.Context, msg kafka.Message) {
	// 创建 Span 追踪消息处理
	ctx, span := observability.StartSpan(spanCtx, "worker.processMessage")
	defer span.End()

	startTime := time.Now()
	bullet, err := protocol.Decode(msg.Value)
	if err != nil {
		logger.Warn("Failed to decode message",
			zap.ByteString("raw_data", msg.Value),
			zap.Error(err))
		observability.RecordError(span, err, "worker.processMessage")
		return
	}
	defer bullet.Release()

	span.SetAttributes(
		attribute.Int64("live_id", int64(bullet.LiveID)),
		attribute.Int64("user_id", int64(bullet.UserID)),
		attribute.String("user_name", bullet.UserName),
		attribute.String("content", bullet.Content),
	)
	logger.Debug("Message decoded",
		zap.Uint64("liveID", bullet.LiveID),
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.UserName),
		zap.String("content", bullet.Content),
		zap.Duration("decode_time", time.Since(startTime)))

	bullet.Color = util.GetDefaultColorOrRandom(bullet.Color)

	currentTime := time.Now().UnixMilli()
	if currentTime-bullet.Timestamp > (3600 * 1000) {
		logger.Warn("Discarding old message",
			zap.Int64("timestamp", bullet.Timestamp),
			zap.Int64("current_time", currentTime),
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.UserName))
		span.AddEvent("discarded_old_message",
			trace.WithAttributes(
				attribute.Int64("timestamp", bullet.Timestamp),
				attribute.Int64("age_ms", currentTime-bullet.Timestamp),
			))
		return
	}
	logger.Debug("Timestamp validated",
		zap.Int64("timestamp", bullet.Timestamp),
		zap.Int64("age_ms", currentTime-bullet.Timestamp))

	if len(bullet.Content) == 0 || len(bullet.Content) > 200 {
		logger.Warn("Invalid content length",
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.UserName),
			zap.Int("content_length", len(bullet.Content)))
		span.AddEvent("invalid_content_length",
			trace.WithAttributes(
				attribute.Int("content_length", len(bullet.Content)),
			))
		return
	}
	logger.Debug("Content length validated",
		zap.Int("content_length", len(bullet.Content)))

	if !w.rateLimit(ctx, bullet.UserID, bullet.UserName) {
		logger.Warn("Rate limit exceeded",
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.UserName))
		span.AddEvent("rate_limit_exceeded")
		return
	}
	logger.Debug("Rate limit passed",
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.UserName))

	w.storeMessage(ctx, bullet)
	logger.Debug("Message processing completed",
		zap.Uint64("liveID", bullet.LiveID),
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.UserName),
		zap.Duration("total_processing_time", time.Since(startTime)))
	observability.RecordLatency(ctx, "worker.processMessage", time.Since(startTime))
}

func (w *Worker) rateLimit(ctx context.Context, userID uint64, userName string) bool {
	// 创建 Span 追踪限流检查
	ctx, span := observability.StartSpan(ctx, "worker.rateLimit",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(userID)),
			attribute.String("user_name", userName),
		))
	defer span.End()

	key := w.keyBuilder.RateLimitKey(userID)
	logger.Debug("Applying rate limit",
		zap.String("key", key),
		zap.Uint64("userID", userID),
		zap.String("userName", userName))

	script := redis.NewScript(`
        local count = redis.call("INCR", KEYS[1])
        if count == 1 then
            redis.call("EXPIRE", KEYS[1], 10)
        end
        if count > 100000000 then
            return 0
        end
        return 1
    `)
	startTime := time.Now()
	result, err := script.Run(ctx, w.redisClient, []string{key}).Int()
	if err != nil {
		logger.Error("Rate limit script failed", zap.Error(err), zap.String("key", key))
		observability.RecordError(span, err, "worker.rateLimit")
		return false
	}
	observability.RecordLatency(ctx, "redis.ScriptRun", time.Since(startTime))
	span.SetAttributes(attribute.Int("count", result))
	logger.Debug("Rate limit script executed",
		zap.String("key", key),
		zap.Int("count", result),
		zap.Duration("execution_time", time.Since(startTime)))
	return result == 1
}

func (w *Worker) storeMessage(spanCtx context.Context, msg *protocol.BulletMessage) {
	// 创建 Span 追踪消息存储
	ctx, span := observability.StartSpan(spanCtx, "worker.storeMessage",
		trace.WithAttributes(
			attribute.Int64("live_id", int64(msg.LiveID)),
			attribute.Int64("user_id", int64(msg.UserID)),
			attribute.String("user_name", msg.UserName),
		))
	defer span.End()

	startTime := time.Now()
	pipe := w.redisClient.Pipeline()

	serializedBullet := protocol.SerializedBullet{
		Timestamp: msg.Timestamp,
		UserID:    msg.UserID,
		LiveID:    msg.LiveID,
		UserName:  msg.UserName,
		Content:   msg.Content,
		Color:     msg.Color, // 包含颜色信息
	}

	serializedData, err := json.Marshal(serializedBullet)
	if err != nil {
		logger.Error("Failed to marshal serialized bullet", zap.Error(err))
		observability.RecordError(span, err, "worker.storeMessage")
		return
	}

	bulletKey := w.keyBuilder.LiveBulletKey(msg.LiveID)
	pipe.LPush(ctx, bulletKey, string(serializedData))
	pipe.LTrim(ctx, bulletKey, 0, 999)
	logger.Debug("Prepared bullet storage",
		zap.String("bullet_key", bulletKey),
		zap.String("serialized_data", string(serializedData)))

	rankingKey := w.keyBuilder.LiveRankingKey(msg.LiveID)
	pipe.ZIncrBy(ctx, rankingKey, 1, w.keyBuilder.UserIDStr(msg.UserID))
	logger.Debug("Prepared ranking update",
		zap.String("ranking_key", rankingKey),
		zap.String("userID_str", w.keyBuilder.UserIDStr(msg.UserID)))

	_, err = pipe.Exec(ctx)
	if err != nil {
		logger.Error("Failed to store message in Redis",
			zap.Uint64("liveID", msg.LiveID),
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.UserName),
			zap.Error(err))
		observability.RecordError(span, err, "worker.storeMessage")
		return
	}
	observability.RecordLatency(ctx, "redis.PipelineExec", time.Since(startTime))
	logger.Debug("Redis pipeline executed",
		zap.String("bullet_key", bulletKey),
		zap.String("ranking_key", rankingKey),
		zap.Duration("execution_time", time.Since(startTime)))

	logger.Info("Message stored",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.UserName),
		zap.String("content", msg.Content),
		zap.String("serialized_data", string(serializedData)))
}

func (w *Worker) Close(spanCtx context.Context) {
	// 创建 Span 追踪 Worker 关闭
	ctx, span := observability.StartSpan(spanCtx, "worker.Close")
	defer span.End()

	logger.Debug("Closing worker resources...")
	startTime := time.Now()

	if err := w.kafkaReader.Close(); err != nil {
		logger.Error("Failed to close Kafka reader", zap.Error(err))
		observability.RecordError(span, err, "worker.Close")
	}
	if err := w.redisClient.Close(); err != nil {
		logger.Error("Failed to close Redis client", zap.Error(err))
		observability.RecordError(span, err, "worker.Close")
	}
	w.wg.Wait()
	observability.RecordLatency(ctx, "worker.Close", time.Since(startTime))
	logger.Debug("Worker resources closed successfully")
}
