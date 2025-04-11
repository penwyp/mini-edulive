package worker

import (
	"context"
	"sync"
	"time"

	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"

	"github.com/penwyp/mini-edulive/config"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Worker struct {
	config      *config.Config
	kafkaReader *kafka.Reader
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder // Add key builder
	wg          sync.WaitGroup
}

func NewWorker(cfg *config.Config) (*Worker, error) {
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		return nil, err
	}
	return &Worker{
		config:      cfg,
		kafkaReader: pkgkafka.NewReader(&cfg.Kafka),
		redisClient: redisClient,
		keyBuilder:  pkgcache.NewRedisKeyBuilder(), // Initialize key builder
	}, nil
}

func (w *Worker) Start(ctx context.Context) {
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
				startTime := time.Now()
				msg, err := w.kafkaReader.ReadMessage(ctx)
				if err != nil {
					logger.Error("Failed to read Kafka message", zap.Error(err))
					continue
				}
				logger.Debug("Kafka message received",
					zap.Int64("offset", msg.Offset),
					zap.Int("partition", msg.Partition),
					zap.ByteString("key", msg.Key),
					zap.Duration("read_latency", time.Since(startTime)))
				w.processMessage(ctx, msg)
			}
		}
	}()
}

func (w *Worker) processMessage(ctx context.Context, msg kafka.Message) {
	startTime := time.Now()
	bullet, err := protocol.Decode(msg.Value)
	if err != nil {
		logger.Warn("Failed to decode message",
			zap.ByteString("raw_data", msg.Value),
			zap.Error(err))
		return
	}
	logger.Debug("Message decoded",
		zap.Uint64("liveID", bullet.LiveID),
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.Username),
		zap.String("content", bullet.Content),
		zap.Duration("decode_time", time.Since(startTime)))

	currentTime := time.Now().UnixMilli()
	if currentTime-bullet.Timestamp > (3600 * 1000) {
		logger.Warn("Discarding old message",
			zap.Int64("timestamp", bullet.Timestamp),
			zap.Int64("current_time", currentTime),
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.Username))
		return
	}
	logger.Debug("Timestamp validated",
		zap.Int64("timestamp", bullet.Timestamp),
		zap.Int64("age_ms", currentTime-bullet.Timestamp))

	if len(bullet.Content) == 0 || len(bullet.Content) > 200 {
		logger.Warn("Invalid content length",
			zap.Uint64("userID", bullet.UserID),
			zap.Int("content_length", len(bullet.Content)))
		return
	}
	logger.Debug("Content length validated",
		zap.Int("content_length", len(bullet.Content)))

	if !w.rateLimit(ctx, bullet.UserID, bullet.Username) {
		logger.Warn("Rate limit exceeded",
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.Username))
		return
	}
	logger.Debug("Rate limit passed",
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.Username))

	w.storeMessage(ctx, bullet)
	logger.Debug("Message processing completed",
		zap.Uint64("liveID", bullet.LiveID),
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.Username),
		zap.Duration("total_processing_time", time.Since(startTime)))
}

func (w *Worker) rateLimit(ctx context.Context, userID uint64, userName string) bool {
	key := w.keyBuilder.RateLimitKey(userID)
	logger.Debug("Applying rate limit",
		zap.String("key", key),
		zap.Uint64("userID", userID),
		zap.String("userName", userName),
	)

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
		return false
	}
	logger.Debug("Rate limit script executed",
		zap.String("key", key),
		zap.Int("count", result),
		zap.Duration("execution_time", time.Since(startTime)))
	return result == 1
}

func (w *Worker) storeMessage(ctx context.Context, msg *protocol.BulletMessage) {
	startTime := time.Now()
	pipe := w.redisClient.Pipeline()

	bulletKey := w.keyBuilder.LiveBulletKey(msg.LiveID)
	pipe.LPush(ctx, bulletKey, msg.Content)
	pipe.LTrim(ctx, bulletKey, 0, 999)
	logger.Debug("Prepared bullet storage",
		zap.String("bullet_key", bulletKey),
		zap.String("content", msg.Content))

	rankingKey := w.keyBuilder.LiveRankingKey(msg.LiveID)
	pipe.ZIncrBy(ctx, rankingKey, 1, w.keyBuilder.UserIDStr(msg.UserID))
	logger.Debug("Prepared ranking update",
		zap.String("ranking_key", rankingKey),
		zap.String("userID_str", w.keyBuilder.UserIDStr(msg.UserID)))

	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.Error("Failed to store message in Redis",
			zap.Uint64("liveID", msg.LiveID),
			zap.Uint64("userID", msg.UserID),
			zap.String("userName", msg.Username),
			zap.Error(err))
		return
	}
	logger.Debug("Redis pipeline executed",
		zap.String("bullet_key", bulletKey),
		zap.String("ranking_key", rankingKey),
		zap.Duration("execution_time", time.Since(startTime)))

	logger.Info("Message stored",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("userName", msg.Username),
		zap.String("content", msg.Content))
}

func (w *Worker) Close() {
	logger.Debug("Closing worker resources...")
	if err := w.kafkaReader.Close(); err != nil {
		logger.Error("Failed to close Kafka reader", zap.Error(err))
	}
	if err := w.redisClient.Close(); err != nil {
		logger.Error("Failed to close Redis client", zap.Error(err))
	}
	w.wg.Wait()
	logger.Debug("Worker resources closed successfully")
}
