package dispatcher

import (
	"context"
	"strconv"
	"time"

	ob "github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/internal/core/websocket"
	pkgcache "github.com/penwyp/mini-edulive/pkg/cache"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/util"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// RoomManager 管理直播房间相关的操作
type RoomManager struct {
	redisClient *redis.ClusterClient
	keyBuilder  *pkgcache.RedisKeyBuilder
}

// NewRoomManager 创建一个新的房间管理器
func NewRoomManager(redisClient *redis.ClusterClient, keyBuilder *pkgcache.RedisKeyBuilder) *RoomManager {
	return &RoomManager{
		redisClient: redisClient,
		keyBuilder:  keyBuilder,
	}
}

// GetActiveRooms 获取所有活跃的房间ID
func (m *RoomManager) GetActiveRooms(ctx context.Context) ([]uint64, error) {
	ctx, span := ob.StartSpan(ctx, "RoomManager.GetActiveRooms")
	defer span.End()

	startTime := time.Now()

	// 从Redis获取活跃房间列表
	activeRoomsKey := m.keyBuilder.ActiveLiveRoomsKey()
	activeRooms, err := m.redisClient.SMembers(ctx, activeRoomsKey).Result()
	if err != nil {
		ob.RecordError(span, err, "RoomManager.GetActiveRooms")
		ob.RedisOperations.WithLabelValues("smembers", "failed").Inc()
		return nil, err
	}

	ob.RedisOperations.WithLabelValues("smembers", "success").Inc()
	ob.RecordLatency(ctx, "redis.SMembers", time.Since(startTime))

	// 如果没有活跃房间，返回后门房间ID
	if len(activeRooms) == 0 {
		return []uint64{websocket.BackDoorLiveRoomID}, nil
	}

	// 将字符串房间ID转换为uint64
	result := make([]uint64, 0, len(activeRooms))
	for _, roomStr := range activeRooms {
		liveID, err := strconv.ParseUint(roomStr, 10, 64)
		if err != nil {
			logger.Warn("Invalid liveID in active rooms",
				zap.String("liveID", roomStr),
				zap.Error(err))
			continue
		}
		result = append(result, liveID)
	}

	// 确保后门房间ID也在列表中
	hasBadDoorID := false
	for _, id := range result {
		if id == websocket.BackDoorLiveRoomID {
			hasBadDoorID = true
			break
		}
	}

	if !hasBadDoorID {
		result = append(result, websocket.BackDoorLiveRoomID)
	}

	return result, nil
}

// IsRoomActive 检查房间是否活跃
func (m *RoomManager) IsRoomActive(ctx context.Context, liveID uint64) (bool, error) {
	ctx, span := ob.StartSpan(ctx, "RoomManager.IsRoomActive")
	defer span.End()

	// 后门房间ID总是活跃的
	if liveID == websocket.BackDoorLiveRoomID {
		return true, nil
	}

	startTime := time.Now()
	activeRoomsKey := m.keyBuilder.ActiveLiveRoomsKey()

	// 检查房间ID是否在活跃集合中
	exists, err := m.redisClient.SIsMember(ctx, activeRoomsKey, util.FormatUint64ToString(liveID)).Result()
	if err != nil {
		ob.RecordError(span, err, "RoomManager.IsRoomActive")
		ob.RedisOperations.WithLabelValues("sismember", "failed").Inc()
		return false, err
	}

	ob.RedisOperations.WithLabelValues("sismember", "success").Inc()
	ob.RecordLatency(ctx, "redis.SIsMember", time.Since(startTime))

	return exists, nil
}

// GetRoomOptions 获取房间的配置选项
func (m *RoomManager) GetRoomOptions(ctx context.Context, liveID uint64) (map[string]string, error) {
	// 这是一个扩展点，可以从Redis或其他存储中获取房间的特定配置
	// 目前返回一个空的配置映射
	return map[string]string{}, nil
}
