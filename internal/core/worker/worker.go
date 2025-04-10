package worker

import (
	"context"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type Worker struct {
	config      *config.Config
	kafkaReader *kafka.Reader
	redisClient *redis.Client
	wg          sync.WaitGroup
}

func NewWorker(cfg *config.Config) (*Worker, error) {
	// Initialize Kafka reader
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Kafka.Brokers,
		Topic:    cfg.Kafka.Topic,
		GroupID:  cfg.Kafka.GroupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})

	// Initialize Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}

	return &Worker{
		config:      cfg,
		kafkaReader: kafkaReader,
		redisClient: redisClient,
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
				msg, err := w.kafkaReader.ReadMessage(ctx)
				if err != nil {
					logger.Error("Failed to read Kafka message", zap.Error(err))
					continue
				}
				w.processMessage(ctx, msg)
			}
		}
	}()
}

func (w *Worker) processMessage(ctx context.Context, msg kafka.Message) {
	// Decode message
	bullet, err := protocol.Decode(msg.Value)
	if err != nil {
		logger.Warn("Failed to decode message", zap.Error(err))
		return
	}

	// Validate timestamp (e.g., discard messages older than 5 seconds)
	if time.Now().UnixMilli()-bullet.Timestamp > 5000 {
		logger.Warn("Discarding old message",
			zap.Int64("timestamp", bullet.Timestamp),
			zap.Uint64("userID", bullet.UserID))
		return
	}

	// Filter content (simple example: discard empty or too long content)
	if len(bullet.Content) == 0 || len(bullet.Content) > 200 {
		logger.Warn("Invalid content length",
			zap.Uint64("userID", bullet.UserID),
			zap.Int("content_length", len(bullet.Content)))
		return
	}

	// Rate limiting using Lua script
	if !w.rateLimit(ctx, bullet.UserID) {
		logger.Warn("Rate limit exceeded", zap.Uint64("userID", bullet.UserID))
		return
	}

	// Batch write to Redis
	w.storeMessage(ctx, bullet)
}

func (w *Worker) rateLimit(ctx context.Context, userID uint64) bool {
	key := "rate:user:" + string(userID)
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
	result, err := script.Run(ctx, w.redisClient, []string{key}).Int()
	if err != nil {
		logger.Error("Rate limit script failed", zap.Error(err))
		return false
	}
	return result == 1
}

func (w *Worker) storeMessage(ctx context.Context, msg *protocol.BulletMessage) {
	pipe := w.redisClient.Pipeline()

	// Store bullet in a list per live room (e.g., "live:<liveID>:bullets")
	bulletKey := "live:" + string(msg.LiveID) + ":bullets"
	pipe.LPush(ctx, bulletKey, msg.Content)
	pipe.LTrim(ctx, bulletKey, 0, 999) // Keep only the latest 1000 bullets

	// Update user activity ranking (e.g., "live:<liveID>:ranking")
	rankingKey := "live:" + string(msg.LiveID) + ":ranking"
	pipe.ZIncrBy(ctx, rankingKey, 1, string(msg.UserID))

	// Execute pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		logger.Error("Failed to store message in Redis",
			zap.Uint64("liveID", msg.LiveID),
			zap.Uint64("userID", msg.UserID),
			zap.Error(err))
		return
	}

	logger.Info("Message stored",
		zap.Uint64("liveID", msg.LiveID),
		zap.Uint64("userID", msg.UserID),
		zap.String("content", msg.Content))
}

func (w *Worker) Close() {
	if err := w.kafkaReader.Close(); err != nil {
		logger.Error("Failed to close Kafka reader", zap.Error(err))
	}
	if err := w.redisClient.Close(); err != nil {
		logger.Error("Failed to close Redis client", zap.Error(err))
	}
	w.wg.Wait()
}
