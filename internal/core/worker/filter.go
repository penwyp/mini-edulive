package worker

import (
	"context"
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

// Filter defines the interface for message filters
type Filter interface {
	Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string)
	Name() string
}

// FilterChain manages a sequence of filters
type FilterChain struct {
	filters []Filter
}

// NewFilterChain creates a new filter chain with the given filters
func NewFilterChain(filters ...Filter) *FilterChain {
	return &FilterChain{
		filters: filters,
	}
}

// Apply runs a message through all filters in the chain
func (fc *FilterChain) Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string) {
	ctx, span := observability.StartSpan(ctx, "FilterChain.Apply")
	defer span.End()

	for _, filter := range fc.filters {
		startTime := time.Now()
		filterCtx, filterSpan := observability.StartSpan(ctx, "Filter."+filter.Name())

		passed, reason := filter.Apply(filterCtx, bullet)

		filterSpan.SetAttributes(
			attribute.Bool("passed", passed),
			attribute.String("reason", reason),
		)
		observability.RecordLatency(filterCtx, "filter."+filter.Name(), time.Since(startTime))
		filterSpan.End()

		if !passed {
			return false, reason
		}
	}

	return true, ""
}

// AgeFilter filters out messages that are too old
type AgeFilter struct {
	maxAgeMs int64
}

// NewAgeFilter creates a new age filter
func NewAgeFilter() *AgeFilter {
	return &AgeFilter{
		maxAgeMs: 3600 * 1000, // 1 hour
	}
}

func (f *AgeFilter) Name() string {
	return "AgeFilter"
}

func (f *AgeFilter) Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string) {
	currentTime := time.Now().UnixMilli()
	if currentTime-bullet.Timestamp > f.maxAgeMs {
		logger.Warn("Discarding old message",
			zap.Int64("timestamp", bullet.Timestamp),
			zap.Int64("current_time", currentTime),
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.UserName))
		observability.BulletFiltered.WithLabelValues("old_message").Inc()
		return false, "message_too_old"
	}

	return true, ""
}

// ContentLengthFilter filters messages based on length
type ContentLengthFilter struct {
	minLength int
	maxLength int
}

// NewContentLengthFilter creates a new content length filter
func NewContentLengthFilter() *ContentLengthFilter {
	return &ContentLengthFilter{
		minLength: 1,
		maxLength: 200,
	}
}

func (f *ContentLengthFilter) Name() string {
	return "ContentLengthFilter"
}

func (f *ContentLengthFilter) Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string) {
	contentLength := len(bullet.Content)
	if contentLength < f.minLength || contentLength > f.maxLength {
		logger.Warn("Invalid content length",
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.UserName),
			zap.Int("content_length", contentLength))
		observability.BulletFiltered.WithLabelValues("invalid_length").Inc()
		return false, "invalid_content_length"
	}

	return true, ""
}

// RateLimiter interface allows for different rate limiting implementations
type RateLimiter interface {
	Allow(ctx context.Context, userID uint64, userName string) bool
}

// RedisRateLimiter implements rate limiting using Redis
type RedisRateLimiter struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewDefaultRateLimiter creates a default rate limiter
func NewDefaultRateLimiter() RateLimiter {
	return &NoopRateLimiter{}
}

// NoopRateLimiter is a rate limiter that allows all messages (for testing)
type NoopRateLimiter struct{}

func (l *NoopRateLimiter) Allow(ctx context.Context, userID uint64, userName string) bool {
	return true
}

// RateLimitFilter filters messages based on rate limiting
type RateLimitFilter struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewRateLimitFilter creates a new rate limit filter
func NewRateLimitFilter(redisClient *redis.ClusterClient, keyBuilder *pkgcache.RedisKeyBuilder) *RateLimitFilter {
	return &RateLimitFilter{
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
	}
}

func (f *RateLimitFilter) Name() string {
	return "RateLimitFilter"
}

func (f *RateLimitFilter) Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string) {
	ctx, span := observability.StartSpan(ctx, "RateLimitFilter.Apply",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(bullet.UserID)),
			attribute.String("user_name", bullet.UserName),
		))
	defer span.End()

	key := f.keyBuilder.RateLimitKey(bullet.UserID)
	logger.Debug("Applying rate limit",
		zap.String("key", key),
		zap.Uint64("userID", bullet.UserID),
		zap.String("userName", bullet.UserName))

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
	result, err := script.Run(ctx, f.redisClient, []string{key}).Int()
	if err != nil {
		logger.Error("Rate limit script failed", zap.Error(err), zap.String("key", key))
		observability.RecordError(span, err, "RateLimitFilter.Apply")
		observability.RedisOperations.WithLabelValues("script", "failed").Inc()
		return false, "rate_limit_error"
	}
	observability.RedisOperations.WithLabelValues("script", "success").Inc()
	observability.RecordLatency(ctx, "redis.ScriptRun", time.Since(startTime))
	span.SetAttributes(attribute.Int("count", result))
	logger.Debug("Rate limit script executed",
		zap.String("key", key),
		zap.Int("count", result),
		zap.Duration("execution_time", time.Since(startTime)))
	if result == 0 {
		observability.RateLimitRejections.WithLabelValues(f.keyBuilder.UserIDStr(bullet.UserID)).Inc()
		observability.BulletFiltered.WithLabelValues("rate_limit").Inc()
		return false, "rate_limit_exceeded"
	}

	return true, ""
}

// SensitiveWordFilter filters messages containing sensitive words
type SensitiveWordFilter struct {
	sensitiveWords []string
}

// NewSensitiveWordFilter creates a new sensitive word filter
func NewSensitiveWordFilter(sensitiveWords []string) *SensitiveWordFilter {
	return &SensitiveWordFilter{
		sensitiveWords: sensitiveWords,
	}
}

func (f *SensitiveWordFilter) Name() string {
	return "SensitiveWordFilter"
}

func (f *SensitiveWordFilter) Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string) {
	for _, word := range f.sensitiveWords {
		if util.ContainsCaseInsensitive(bullet.Content, word) {
			logger.Warn("Sensitive word detected",
				zap.Uint64("userID", bullet.UserID),
				zap.String("userName", bullet.UserName),
				zap.String("content", bullet.Content))
			observability.BulletFiltered.WithLabelValues("sensitive_word").Inc()
			return false, "sensitive_word_detected"
		}
	}

	return true, ""
}

// DuplicateFilter filters duplicate messages from the same user
type DuplicateFilter struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewDuplicateFilter creates a new duplicate filter
func NewDuplicateFilter(redisClient *redis.ClusterClient, keyBuilder *pkgcache.RedisKeyBuilder) *DuplicateFilter {
	return &DuplicateFilter{
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
	}
}

func (f *DuplicateFilter) Name() string {
	return "DuplicateFilter"
}

func (f *DuplicateFilter) Apply(ctx context.Context, bullet *protocol.BulletMessage) (bool, string) {
	ctx, span := observability.StartSpan(ctx, "DuplicateFilter.Apply",
		trace.WithAttributes(
			attribute.Int64("user_id", int64(bullet.UserID)),
			attribute.String("content", bullet.Content),
		))
	defer span.End()

	key := f.keyBuilder.DuplicateKey(bullet.UserID, bullet.Content)
	startTime := time.Now()
	exists, err := f.redisClient.Exists(ctx, key).Result()
	if err != nil {
		logger.Warn("Failed to check duplicate", zap.Error(err), zap.String("key", key))
		observability.RecordError(span, err, "DuplicateFilter.Apply")
		observability.RedisOperations.WithLabelValues("exists", "failed").Inc()
		return true, "" // Allow on error
	}
	if exists > 0 {
		logger.Warn("Duplicate message detected",
			zap.Uint64("userID", bullet.UserID),
			zap.String("userName", bullet.UserName),
			zap.String("content", bullet.Content))
		observability.BulletFiltered.WithLabelValues("duplicate").Inc()
		return false, "duplicate_message"
	}

	err = f.redisClient.SetEx(ctx, key, "1", 5*time.Second).Err()
	if err != nil {
		logger.Warn("Failed to set duplicate cache", zap.Error(err), zap.String("key", key))
		observability.RecordError(span, err, "DuplicateFilter.Apply")
		observability.RedisOperations.WithLabelValues("setex", "failed").Inc()
	}
	observability.RedisOperations.WithLabelValues("exists", "success").Inc()
	observability.RedisOperations.WithLabelValues("setex", "success").Inc()
	observability.RecordLatency(ctx, "redis.DuplicateCheck", time.Since(startTime))

	return true, ""
}
