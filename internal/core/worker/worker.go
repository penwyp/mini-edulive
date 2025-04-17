package worker

import (
	"context"
	"sync"
	"time"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	pkgkafka "github.com/penwyp/mini-edulive/pkg/kafka"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// Worker manages the processing of bullet messages from Kafka to Redis.
type Worker struct {
	config      *config.Config
	kafkaReader *kafka.Reader
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
	wg          sync.WaitGroup

	// Add new fields for better composition
	messageProcessor *MessageProcessor
	filterChain      *FilterChain
	storage          *BulletStorage
}

// Options for configuring the worker
type Options struct {
	NumWorkers  int
	BufferSize  int
	RateLimiter RateLimiter
}

// DefaultOptions provides sensible defaults
func DefaultOptions() Options {
	return Options{
		NumWorkers:  10,
		BufferSize:  1000,
		RateLimiter: NewDefaultRateLimiter(),
	}
}

// NewWorker creates a new worker instance
func NewWorker(spanCtx context.Context, cfg *config.Config) (*Worker, error) {
	ctx, span := observability.StartSpan(spanCtx, "worker.NewWorker")
	defer span.End()

	startTime := time.Now()
	redisClient, err := pkgcache.NewRedisClusterClient(&cfg.Redis)
	if err != nil {
		logger.Error("Failed to create Redis client", zap.Error(err))
		observability.RecordError(span, err, "worker.NewWorker")
		observability.RedisOperations.WithLabelValues("connect", "failed").Inc()
		return nil, err
	}
	observability.RedisOperations.WithLabelValues("connect", "success").Inc()
	observability.RecordLatency(ctx, "redis.NewClusterClient", time.Since(startTime))

	keyBuilder := pkgcache.NewRedisKeyBuilder()

	// Create component instances
	storage := NewBulletStorage(redisClient, keyBuilder)

	// Create filter chain with all filters
	filterChain := NewFilterChain(
		NewAgeFilter(),
		NewContentLengthFilter(),
		NewRateLimitFilter(redisClient, keyBuilder),
		NewSensitiveWordFilter([]string{"badword", "spam", "offensive"}),
		NewDuplicateFilter(redisClient, keyBuilder),
	)

	// Create message processor
	messageProcessor := NewMessageProcessor(filterChain, storage)

	w := &Worker{
		config:           cfg,
		kafkaReader:      pkgkafka.NewReader(&cfg.Kafka),
		redisClient:      redisClient,
		keyBuilder:       keyBuilder,
		messageProcessor: messageProcessor,
		filterChain:      filterChain,
		storage:          storage,
	}

	return w, nil
}

// Start begins consuming messages from Kafka and processing them
func (w *Worker) Start(spanCtx context.Context) {
	ctx, span := observability.StartSpan(spanCtx, "worker.Start")
	defer span.End()

	logger.Info("Worker started, consuming Kafka messages...")

	w.wg.Add(1)
	go w.consumeMessages(ctx)
}

// consumeMessages handles the Kafka message consumption loop
func (w *Worker) consumeMessages(ctx context.Context) {
	defer w.wg.Done()

	// Use a buffered channel to manage backpressure
	msgChan := make(chan kafka.Message, DefaultOptions().BufferSize)
	workerPool := make(chan struct{}, DefaultOptions().NumWorkers)
	var workerWG sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < DefaultOptions().NumWorkers; i++ {
		workerWG.Add(1)
		go func(workerId int) {
			defer workerWG.Done()

			for msg := range msgChan {
				workerPool <- struct{}{}
				w.handleMessage(msg, workerId)
				<-workerPool
			}
		}(i)
	}

	// Main message consumption loop
	for {
		select {
		case <-ctx.Done():
			logger.Info("Worker context canceled, stopping...", zap.Error(ctx.Err()))
			close(msgChan)
			workerWG.Wait()
			return
		default:
			msgCtx, msgSpan := observability.StartSpan(context.Background(), "worker.consumeMessage")
			startTime := time.Now()

			msg, err := w.kafkaReader.ReadMessage(msgCtx)
			if err != nil {
				if err == context.DeadlineExceeded {
					logger.Debug("Kafka read timeout, continuing...")
				} else {
					logger.Error("Failed to read Kafka message", zap.Error(err))
					observability.RecordError(msgSpan, err, "worker.consumeMessage")
				}
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

			observability.KafkaMessagesReceived.WithLabelValues("worker").Inc()

			select {
			case msgChan <- msg:
			default:
				logger.Warn("Message channel full, dropping message")
				observability.KafkaMessagesDropped.WithLabelValues("worker").Inc()
			}

			msgSpan.End()
		}
	}
}

// handleMessage processes a single Kafka message
func (w *Worker) handleMessage(msg kafka.Message, workerId int) {
	ctx := context.Background()
	msgCtx, span := observability.StartSpan(ctx, "worker.handleMessage")
	defer span.End()

	span.SetAttributes(attribute.Int("worker_id", workerId))
	startTime := time.Now()

	// Decode the message
	bullet, err := w.decodeMessage(msgCtx, msg)
	if err != nil {
		observability.BulletMessagesProcessed.WithLabelValues("worker", "failed").Inc()
		return
	}
	defer bullet.Release()

	// Process the message through the processor
	if err := w.messageProcessor.Process(msgCtx, bullet); err != nil {
		logger.Warn("Failed to process message",
			zap.Uint64("liveID", bullet.LiveID),
			zap.Uint64("userID", bullet.UserID),
			zap.Error(err))
		return
	}

	observability.BulletMessagesProcessed.WithLabelValues("worker", "success").Inc()
	logger.Debug("Message processing completed",
		zap.Uint64("liveID", bullet.LiveID),
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.UserName),
		zap.Duration("total_processing_time", time.Since(startTime)))
	observability.RecordLatency(ctx, "worker.handleMessage", time.Since(startTime))
}

// decodeMessage decodes a Kafka message into a BulletMessage
func (w *Worker) decodeMessage(ctx context.Context, msg kafka.Message) (*protocol.BulletMessage, error) {
	ctx, span := observability.StartSpan(ctx, "worker.decodeMessage")
	defer span.End()

	startTime := time.Now()
	bullet, err := protocol.Decode(msg.Value)
	if err != nil {
		logger.Warn("Failed to decode message",
			zap.ByteString("raw_data", msg.Value),
			zap.Error(err))
		observability.RecordError(span, err, "worker.decodeMessage")
		observability.ProtocolParseErrors.WithLabelValues("bullet").Inc()
		return nil, err
	}

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
	observability.RecordLatency(ctx, "worker.decodeMessage", time.Since(startTime))
	return bullet, nil
}

// Close shuts down the worker and all associated resources
func (w *Worker) Close(spanCtx context.Context) {
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
		observability.RedisOperations.WithLabelValues("close", "failed").Inc()
	}
	w.wg.Wait()
	observability.RedisOperations.WithLabelValues("close", "success").Inc()
	observability.RecordLatency(ctx, "worker.Close", time.Since(startTime))
	logger.Debug("Worker resources closed successfully")
}
