package dispatcher

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/pool"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// BulletData 存储弹幕数据和元信息
type BulletData struct {
	LiveID     uint64
	UniqueKey  string
	Bullet     *protocol.BulletMessage
	Serialized *protocol.SerializedBullet
}

// 对象池注册
var (
	bulletSlicePool = pool.RegisterPool("bullet_slice", func() []*protocol.BulletMessage {
		return make([]*protocol.BulletMessage, 0, 100)
	})
	serializedBulletPool = pool.RegisterPool("serialized_bullet", func() *protocol.SerializedBullet {
		return &protocol.SerializedBullet{}
	})
	bulletDataSlicePool = pool.RegisterPool("bullet_data_slice", func() []*BulletData {
		return make([]*BulletData, 0, 100)
	})
)

// BulletManager 管理弹幕相关的操作
type BulletManager struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
	compression bool
}

// NewBulletManager 创建一个新的弹幕管理器
func NewBulletManager(redisClient *redis.ClusterClient, keyBuilder *pkgcache.RedisKeyBuilder, compression bool) *BulletManager {
	return &BulletManager{
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
		compression: compression,
	}
}

// FetchBulletsByRoom 获取每个房间的最新弹幕
func (m *BulletManager) FetchBulletsByRoom(ctx context.Context, roomIDs []uint64, maxPerRoom int) (map[uint64][]*BulletData, error) {
	ctx, span := ob.StartSpan(ctx, "BulletManager.FetchBulletsByRoom")
	defer span.End()

	startTime := time.Now()
	result := make(map[uint64][]*BulletData)

	// 使用管道批量获取每个房间的弹幕
	pipe := m.redisClient.Pipeline()
	for _, liveID := range roomIDs {
		bulletKey := m.keyBuilder.LiveBulletKey(liveID)

		// 获取最近60秒的弹幕
		minTime := fmt.Sprintf("%d", time.Now().UnixMilli()-60*1000)
		pipe.ZRevRangeByScore(ctx, bulletKey, &redis.ZRangeBy{
			Min:    minTime,
			Max:    "+inf",
			Offset: 0,
			Count:  int64(maxPerRoom),
		})
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		ob.RecordError(span, err, "BulletManager.FetchBulletsByRoom")
		ob.RedisOperations.WithLabelValues("zrevrangebyscore", "failed").Inc()
		return nil, err
	}
	ob.RedisOperations.WithLabelValues("zrevrangebyscore", "success").Inc()

	// 处理获取到的数据
	for i, cmd := range cmds {
		if cmd.Err() != nil && cmd.Err() != redis.Nil {
			logger.Warn("Failed to fetch bullets for liveID",
				zap.Uint64("liveID", roomIDs[i]),
				zap.Error(cmd.Err()))
			continue
		}

		// 获取当前房间ID
		liveID := roomIDs[i]

		// 解析数据
		bulletStrs, _ := cmd.(*redis.StringSliceCmd).Result()
		if len(bulletStrs) == 0 {
			continue
		}

		// 解析每条弹幕数据
		bullets, err := m.parseBulletData(ctx, liveID, bulletStrs)
		if err != nil {
			logger.Error("Failed to parse bullet data",
				zap.Uint64("liveID", liveID),
				zap.Error(err))
			continue
		}

		// 过滤已处理的弹幕
		filteredBullets, err := m.filterProcessedBullets(ctx, bullets)
		if err != nil {
			logger.Error("Failed to filter processed bullets",
				zap.Uint64("liveID", liveID),
				zap.Error(err))
			continue
		}

		if len(filteredBullets) > 0 {
			result[liveID] = filteredBullets
		}
	}

	ob.RecordLatency(ctx, "BulletManager.FetchBulletsByRoom", time.Since(startTime))
	return result, nil
}

// parseBulletData 解析Redis中存储的弹幕数据
func (m *BulletManager) parseBulletData(ctx context.Context, liveID uint64, bulletStrs []string) ([]*BulletData, error) {
	ctx, span := ob.StartSpan(ctx, "BulletManager.parseBulletData")
	defer span.End()

	bullets := bulletDataSlicePool.Get()
	bullets = bullets[:0]

	for _, data := range bulletStrs {
		// 提取序列化数据（格式为 userID:timestamp:serializedData）
		parts := strings.SplitN(data, ":", 3)
		if len(parts) < 3 {
			logger.Warn("Invalid bullet data format",
				zap.String("data", data),
				zap.Uint64("liveID", liveID))
			ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
			continue
		}

		serializedData := parts[2]
		serializedBullet := serializedBulletPool.Get()
		serializedBullet.Reset()

		if err := json.Unmarshal([]byte(serializedData), serializedBullet); err != nil {
			logger.Warn("Failed to unmarshal serialized bullet",
				zap.String("serialized_data", serializedData),
				zap.Error(err))
			ob.RecordError(span, err, "BulletManager.parseBulletData")
			ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
			serializedBulletPool.Put(serializedBullet)
			continue
		}

		// 创建弹幕消息对象
		bullet := protocol.NewBulletMessage(
			serializedBullet.LiveID,
			serializedBullet.UserID,
			serializedBullet.UserName,
			serializedBullet.Timestamp,
			serializedBullet.Content,
			util.GetDefaultColorOrRandom(serializedBullet.Color),
		)

		// 设置时间戳
		bullet.Timestamp = serializedBullet.Timestamp

		// 生成唯一键
		uniqueKey := bullet.UniqueKey()

		// 添加到结果
		bullets = append(bullets, &BulletData{
			LiveID:     liveID,
			UniqueKey:  uniqueKey,
			Bullet:     bullet,
			Serialized: serializedBullet,
		})
	}

	return bullets, nil
}

// filterProcessedBullets 过滤已处理过的弹幕
func (m *BulletManager) filterProcessedBullets(ctx context.Context, bullets []*BulletData) ([]*BulletData, error) {
	ctx, span := ob.StartSpan(ctx, "BulletManager.filterProcessedBullets",
		trace.WithAttributes(
			attribute.Int("bullet_count", len(bullets)),
		))
	defer span.End()

	if len(bullets) == 0 {
		return nil, nil
	}

	// 为每个房间创建检查管道
	pipe := m.redisClient.Pipeline()
	for _, b := range bullets {
		processedKey := m.keyBuilder.ProcessedBulletsKey(b.LiveID)
		pipe.SIsMember(ctx, processedKey, b.UniqueKey)
	}

	// 执行检查
	cmds, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// 清理资源
		for _, b := range bullets {
			serializedBulletPool.Put(b.Serialized)
			b.Bullet.Release()
		}
		bulletDataSlicePool.Put(bullets)

		ob.RecordError(span, err, "BulletManager.filterProcessedBullets")
		ob.RedisOperations.WithLabelValues("sismember", "failed").Inc()
		return nil, err
	}
	ob.RedisOperations.WithLabelValues("sismember", "success").Inc()

	// 过滤未处理的弹幕
	filteredBullets := bulletDataSlicePool.Get()
	filteredBullets = filteredBullets[:0]

	// 标记新的弹幕为已处理
	markPipe := m.redisClient.Pipeline()

	for i, cmd := range cmds {
		if cmd.Err() != nil && cmd.Err() != redis.Nil {
			continue
		}

		processed, _ := cmd.(*redis.BoolCmd).Result()
		if processed {
			// 已处理的弹幕，释放资源
			serializedBulletPool.Put(bullets[i].Serialized)
			bullets[i].Bullet.Release()
			continue
		}

		// 未处理的弹幕，添加到结果
		filteredBullets = append(filteredBullets, bullets[i])

		// 标记为已处理
		processedKey := m.keyBuilder.ProcessedBulletsKey(bullets[i].LiveID)
		markPipe.SAdd(ctx, processedKey, bullets[i].UniqueKey)
		markPipe.Expire(ctx, processedKey, 60*time.Second) // 60秒后过期
	}

	// 执行标记
	if len(filteredBullets) > 0 {
		_, err = markPipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			ob.RecordError(span, err, "BulletManager.filterProcessedBullets")
			ob.RedisOperations.WithLabelValues("sadd", "failed").Inc()
			// 不返回错误，继续处理
		} else {
			ob.RedisOperations.WithLabelValues("sadd", "success").Inc()
		}
	}

	// 清理原始资源
	bulletDataSlicePool.Put(bullets)

	return filteredBullets, nil
}

// EncodeBullets 将多个弹幕编码为二进制数据
func (m *BulletManager) EncodeBullets(spanCtx context.Context, bullets []*BulletData) ([]byte, error) {
	ctx, span := ob.StartSpan(spanCtx, "BulletManager.EncodeBullets",
		trace.WithAttributes(
			attribute.Int("bullet_count", len(bullets)),
		))
	defer span.End()

	// 获取缓冲区
	startTime := time.Now()
	bufPool, err := pool.GetPool[*bytes.Buffer]("bytes_buffer")
	if err != nil {
		ob.RecordError(span, err, "BulletManager.EncodeBullets")
		return nil, fmt.Errorf("failed to get bytes_buffer pool: %v", err)
	}

	buf := bufPool.Get()
	buf.Reset()
	defer bufPool.Put(buf)

	// 编码每个弹幕并写入缓冲区
	for _, b := range bullets {
		logger.Debug("Encoding bullet",
			zap.Uint64("userID", b.Bullet.UserID),
			zap.Uint64("liveID", b.Bullet.LiveID),
			zap.String("content", b.Bullet.Content),
		)
		data, err := b.Bullet.Encode(m.compression)
		if err != nil {
			logger.Warn("Failed to encode bullet", zap.Error(err))
			ob.RecordError(span, err, "BulletManager.EncodeBullets")
			ob.ProtocolParseErrors.WithLabelValues("bullet").Inc()
			continue
		}

		// 写入长度
		if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
			ob.RecordError(span, err, "BulletManager.EncodeBullets")
			return nil, err
		}

		// 写入数据
		buf.Write(data)
	}

	ob.RecordLatency(ctx, "BulletManager.EncodeBullets", time.Since(startTime))
	return buf.Bytes(), nil
}

// ReleaseBullets 释放弹幕资源
func (m *BulletManager) ReleaseBullets(bullets []*BulletData) {
	for _, b := range bullets {
		serializedBulletPool.Put(b.Serialized)
		b.Bullet.Release()
	}
	bulletDataSlicePool.Put(bullets)
}
