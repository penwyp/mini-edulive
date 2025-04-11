// pkg/cache/keys.go
package cache

import "fmt"

type RedisKeyBuilder struct{}

// RateLimitKey 生成用户频率限制的键
func (b *RedisKeyBuilder) RateLimitKey(userID uint64) string {
	return fmt.Sprintf("ratelimit:user:%d", userID)
}

// LiveBulletKey 生成直播间弹幕存储的键
func (b *RedisKeyBuilder) LiveBulletKey(liveID uint64) string {
	return fmt.Sprintf("live:{%d}:bullets", liveID)
}

// LiveRankingKey 生成直播间用户活跃排名的键
func (b *RedisKeyBuilder) LiveRankingKey(liveID uint64) string {
	return fmt.Sprintf("live:{%d}:ranking", liveID)
}

// ActiveLiveRoomsKey 生成活跃直播间列表的键
func (b *RedisKeyBuilder) ActiveLiveRoomsKey() string {
	return "live:active"
}

// UserIDStr 格式化用户 ID 为字符串
func (b *RedisKeyBuilder) UserIDStr(userID uint64) string {
	return fmt.Sprintf("%d", userID)
}

func NewRedisKeyBuilder() *RedisKeyBuilder {
	return &RedisKeyBuilder{}
}
