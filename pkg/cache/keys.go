package cache

import (
	"fmt"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"strconv"
	"strings"
)

var redisKeyBufferPool = pool.RegisterPool("redis_key_buffer", func() *strings.Builder {
	return &strings.Builder{}
})

type RedisKeyBuilder struct{}

// RateLimitKey 生成用户频率限制的键
func (b *RedisKeyBuilder) RateLimitKey(userID uint64) string {
	return fmt.Sprintf("ratelimit:user:%d", userID)
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

// LiveBulletKey 生成直播间弹幕存储的键
func (b *RedisKeyBuilder) LiveBulletKey(liveID uint64) string {
	buf := redisKeyBufferPool.Get()
	buf.Reset() // 重置缓冲区
	defer redisKeyBufferPool.Put(buf)

	buf.WriteString("live:{")
	buf.WriteString(strconv.FormatUint(liveID, 10))
	buf.WriteString("}:bullets")
	return buf.String()
}

// LiveRankingKey 生成直播间用户活跃排名的键
func (b *RedisKeyBuilder) LiveRankingKey(liveID uint64) string {
	buf := redisKeyBufferPool.Get()
	buf.Reset()
	defer redisKeyBufferPool.Put(buf)

	buf.WriteString("live:{")
	buf.WriteString(strconv.FormatUint(liveID, 10))
	buf.WriteString("}:ranking")
	return buf.String()
}
