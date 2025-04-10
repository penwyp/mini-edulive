package cache

import "fmt"

// RedisKeyBuilder provides methods to generate Redis keys consistently
type RedisKeyBuilder struct{}

// NewRedisKeyBuilder creates a new RedisKeyBuilder instance
func NewRedisKeyBuilder() *RedisKeyBuilder {
	return &RedisKeyBuilder{}
}

// RateLimitKey generates a rate limiting key for a user
func (b *RedisKeyBuilder) RateLimitKey(userID uint64) string {
	return fmt.Sprintf("rate:user:%d", userID)
}

// LiveBulletKey generates a key for storing bullets in a live room
func (b *RedisKeyBuilder) LiveBulletKey(liveID uint64) string {
	return fmt.Sprintf("live:{%d}:bullets", liveID)
}

// LiveRankingKey generates a key for user activity rankings in a live room
func (b *RedisKeyBuilder) LiveRankingKey(liveID uint64) string {
	return fmt.Sprintf("live:{%d}:ranking", liveID)
}

// UserIDStr formats a userID as a string (for use in Redis commands)
func (b *RedisKeyBuilder) UserIDStr(userID uint64) string {
	return fmt.Sprintf("%d", userID)
}
