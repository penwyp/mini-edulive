package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/penwyp/mini-edulive/pkg/pool"
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

// DuplicateKey 生成用于重复弹幕检查的 Redis 键
// 参数：
//   - userID: 用户 ID
//   - content: 弹幕内容
//
// 返回：格式为 "duplicate:<userID>:<content_hash>" 的 Redis 键
// 使用场景：在 worker.go 的 isDuplicate 函数中，生成键以检查和记录重复弹幕
func (b *RedisKeyBuilder) DuplicateKey(userID uint64, content string) string {
	// 使用 SHA-256 哈希内容以限制键长度
	hash := sha256.Sum256([]byte(content))
	hashStr := hex.EncodeToString(hash[:8]) // 取前 8 字节以减少键长度
	return fmt.Sprintf("duplicate:%d:%s", userID, hashStr)
}

// ProcessedBulletsKey 生成已处理弹幕的 Redis 键
func (b *RedisKeyBuilder) ProcessedBulletsKey(liveID uint64) string {
	buf := redisKeyBufferPool.Get()
	buf.Reset()
	defer redisKeyBufferPool.Put(buf)

	buf.WriteString("live:{")
	buf.WriteString(strconv.FormatUint(liveID, 10))
	buf.WriteString("}:processed")
	return buf.String()
}
