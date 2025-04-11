package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// 定义全局 Prometheus 指标用于直播弹幕系统的可观测性
var (
	// ActiveWebSocketConnections 跟踪当前活跃的 WebSocket 连接数，按组件分类
	ActiveWebSocketConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "edulive_websocket_connections_active",
			Help: "当前活跃的 WebSocket 连接数",
		},
		[]string{"component"}, // component: gateway, client
	)

	// ActiveQUICConnections 跟踪当前活跃的 QUIC 连接数，按组件分类
	ActiveQUICConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "edulive_quic_connections_active",
			Help: "当前活跃的 QUIC 连接数",
		},
		[]string{"component"}, // component: dispatcher, client
	)

	// WebSocketConnectionErrors 统计 WebSocket 连接错误数，按组件和错误类型分类
	WebSocketConnectionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_websocket_connection_errors_total",
			Help: "WebSocket 连接错误总数",
		},
		[]string{"component", "error_type"}, // error_type: accept, read, write, close
	)

	// QUICConnectionErrors 统计 QUIC 连接错误数，按组件和错误类型分类
	QUICConnectionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_quic_connection_errors_total",
			Help: "QUIC 连接错误总数",
		},
		[]string{"component", "error_type"}, // error_type: accept, read, write, close
	)

	// BulletMessagesProcessed 跟踪处理的弹幕消息总数，按组件和状态分类
	BulletMessagesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_bullet_messages_processed_total",
			Help: "处理的弹幕消息总数",
		},
		[]string{"component", "status"}, // status: success, failed
	)

	// BulletMessageLatency 测量弹幕消息处理延迟（秒），按组件和消息类型分类
	BulletMessageLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_bullet_message_latency_seconds",
			Help:    "弹幕消息处理延迟（秒）",
			Buckets: []float64{0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0}, // 优化桶分布以捕获 100ms 目标
		},
		[]string{"component", "message_type"}, // message_type: bullet, heartbeat, create_room, check_room
	)

	// KafkaMessagesSent 统计发送到 Kafka 的消息数，按状态分类
	KafkaMessagesSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_kafka_messages_sent_total",
			Help: "发送到 Kafka 的消息总数",
		},
		[]string{"status"}, // status: success, failed
	)

	// KafkaMessageLatency 测量 Kafka 消息写入延迟（秒）
	KafkaMessageLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_kafka_message_latency_seconds",
			Help:    "Kafka 消息写入延迟（秒）",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5}, // 关注低延迟场景
		},
		[]string{},
	)

	// RedisOperations 统计 Redis 操作数，按操作类型和状态分类
	RedisOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_redis_operations_total",
			Help: "Redis 操作总数",
		},
		[]string{"operation", "status"}, // operation: sadd, srem, zadd, zrange, etc.; status: success, failed
	)

	// RedisOperationLatency 测量 Redis 操作延迟（秒），按操作类型分类
	RedisOperationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_redis_operation_latency_seconds",
			Help:    "Redis 操作延迟（秒）",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1}, // Redis 操作通常很快
		},
		[]string{"operation"},
	)

	// BulletFiltered 统计被过滤的弹幕数，按过滤类型分类
	BulletFiltered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_bullet_filtered_total",
			Help: "被过滤的弹幕总数",
		},
		[]string{"filter_type"}, // filter_type: sensitive_word, duplicate, rate_limit, low_level
	)

	// LiveRoomCreations 统计直播间创建请求数，按状态分类
	LiveRoomCreations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_live_room_creations_total",
			Help: "直播间创建请求总数",
		},
		[]string{"status"}, // status: success, failed
	)

	// LiveRoomChecks 统计直播间检查请求数，按状态分类
	LiveRoomChecks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_live_room_checks_total",
			Help: "直播间检查请求总数",
		},
		[]string{"status"}, // status: exists, not_exists, failed
	)

	// RankingUpdates 统计排行榜更新操作数，按状态分类
	RankingUpdates = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_ranking_updates_total",
			Help: "排行榜更新操作总数",
		},
		[]string{"status"}, // status: success, failed
	)

	// PoolUsage 跟踪对象池的使用情况，按池类型分类
	PoolUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "edulive_pool_usage",
			Help: "对象池当前使用量",
		},
		[]string{"pool_type"}, // pool_type: bullet_message, bytes_buffer, kafka_key, etc.
	)

	// MemoryPoolAllocations 统计内存池分配次数，按池类型分类
	MemoryPoolAllocations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_memory_pool_allocations_total",
			Help: "内存池分配次数",
		},
		[]string{"pool_type"},
	)

	// CompressionRatio 测量弹幕消息压缩比率
	CompressionRatio = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "edulive_bullet_compression_ratio",
			Help:    "弹幕消息压缩比率（压缩后大小/原始大小）",
			Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
		},
	)

	// ProtocolParseErrors 统计协议解析错误数，按消息类型分类
	ProtocolParseErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_protocol_parse_errors_total",
			Help: "协议解析错误总数",
		},
		[]string{"message_type"}, // message_type: bullet, heartbeat, create_room, check_room
	)

	// RateLimitRejections 统计因频率限制拒绝的弹幕数，按用户ID分类
	RateLimitRejections = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_rate_limit_rejections_total",
			Help: "因频率限制拒绝的弹幕总数",
		},
		[]string{"user_id"},
	)

	// HeartbeatFailures 统计心跳消息失败数，按组件分类
	HeartbeatFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_heartbeat_failures_total",
			Help: "心跳消息失败总数",
		},
		[]string{"component"},
	)

	// metricsInitialized 确保指标只初始化一次
	metricsInitialized bool
)

// InitMetrics 初始化所有 Prometheus 指标（如果尚未初始化）
func InitMetrics() {
	if metricsInitialized {
		return // 防止重复初始化
	}

	// 通过 promauto 在包级别自动注册指标，标记为已初始化
	metricsInitialized = true
}

// RegisterCustomCounter 注册自定义 Counter 指标
func RegisterCustomCounter(name, help string, labels []string) *prometheus.CounterVec {
	return promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_" + name,
			Help: help,
		},
		labels,
	)
}

// RegisterCustomGauge 注册自定义 Gauge 指标
func RegisterCustomGauge(name, help string) prometheus.Gauge {
	return promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "edulive_" + name,
			Help: help,
		},
	)
}

// RegisterCustomHistogram 注册自定义 Histogram 指标
func RegisterCustomHistogram(name, help string, labels []string, buckets []float64) *prometheus.HistogramVec {
	if buckets == nil {
		buckets = prometheus.DefBuckets // 未指定时使用默认桶
	}
	return promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_" + name,
			Help:    help,
			Buckets: buckets,
		},
		labels,
	)
}

// ResetMetrics 重置所有支持重置的指标到初始状态（用于测试或特殊场景）
func ResetMetrics() {
	ActiveWebSocketConnections.Reset()
	ActiveQUICConnections.Reset()
	WebSocketConnectionErrors.Reset()
	QUICConnectionErrors.Reset()
	BulletMessagesProcessed.Reset()
	BulletMessageLatency.Reset()
	KafkaMessagesSent.Reset()
	KafkaMessageLatency.Reset()
	RedisOperations.Reset()
	RedisOperationLatency.Reset()
	BulletFiltered.Reset()
	LiveRoomCreations.Reset()
	LiveRoomChecks.Reset()
	RankingUpdates.Reset()
	PoolUsage.Reset()
	MemoryPoolAllocations.Reset()
	// 注意：CompressionRatio 是 Histogram，不支持 Reset
	ProtocolParseErrors.Reset()
	RateLimitRejections.Reset()
	HeartbeatFailures.Reset()
}
