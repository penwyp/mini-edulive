package worker

import (
	"context"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	rediskeys "github.com/penwyp/mini-edulive/pkg/cache"
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
	keyBuilder  *rediskeys.RedisKeyBuilder // Add key builder
	wg          sync.WaitGroup
}

func NewWorker(cfg *config.Config) (*Worker, error) {
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	logger.Debug("Kafka reader initialized",
		zap.Strings("brokers", cfg.Kafka.Brokers),
		zap.String("topic", cfg.Kafka.Topic),
		zap.String("groupID", cfg.Kafka.GroupID))

	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    cfg.Redis.Addrs,
		Password: cfg.Redis.Password,
	})
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		logger.Error("Failed to ping Redis Cluster", zap.Error(err))
		return nil, err
	}
	logger.Debug("Redis Cluster client initialized",
		zap.Strings("addrs", cfg.Redis.Addrs))

	return &Worker{
		config:      cfg,
		kafkaReader: kafkaReader,
		redisClient: redisClient,
		keyBuilder:  rediskeys.NewRedisKeyBuilder(), // Initialize key builder
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
		zap.String("content", bullet.Content),
		zap.Duration("decode_time", time.Since(startTime)))

	currentTime := time.Now().UnixMilli()
	if currentTime-bullet.Timestamp > (3600 * 1000) {
		logger.Warn("Discarding old message",
			zap.Int64("timestamp", bullet.Timestamp),
			zap.Int64("current_time", currentTime),
			zap.Uint64("userID", bullet.UserID))
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

	if !w.rateLimit(ctx, bullet.UserID) {
		logger.Warn("Rate limit exceeded", zap.Uint64("userID", bullet.UserID))
		return
	}
	logger.Debug("Rate limit passed", zap.Uint64("userID", bullet.UserID))

	w.storeMessage(ctx, bullet)
	logger.Debug("Message processing completed",
		zap.Uint64("liveID", bullet.LiveID),
		zap.Uint64("userID", bullet.UserID),
		zap.Duration("total_processing_time", time.Since(startTime)))
}

func (w *Worker) rateLimit(ctx context.Context, userID uint64) bool {
	key := w.keyBuilder.RateLimitKey(userID)
	logger.Debug("Applying rate limit",
		zap.String("key", key),
		zap.Uint64("userID", userID))

	script := redis.NewScript(`
        local count = redis.call("INCR", KEYS[1])
        if count == 1 then
            redis.call("EXPIRE", KEYS[1], 60)
        end
        if count > 10 then
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
