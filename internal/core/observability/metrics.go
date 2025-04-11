package observability

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// 定义全局 Prometheus 指标用于直播弹幕系统的可观测性
var (
	// ActiveWebSocketConnections 跟踪当前活跃的 WebSocket 连接数，按组件分类
	// 使用场景：在 gateway 和 client 中，连接建立时调用 Inc()，断开时调用 Dec()
	// 示例：ActiveWebSocketConnections.WithLabelValues("gateway").Inc()
	ActiveWebSocketConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "edulive_websocket_connections_active",
			Help: "当前活跃的 WebSocket 连接数",
		},
		[]string{"component"}, // component: gateway, client
	)

	// ActiveQUICConnections 跟踪当前活跃的 QUIC 连接数，按组件分类
	// 使用场景：在 dispatcher 和 client 中，QUIC 连接建立时调用 Inc()，断开时调用 Dec()
	// 示例：ActiveQUICConnections.WithLabelValues("dispatcher").Inc()
	ActiveQUICConnections = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "edulive_quic_connections_active",
			Help: "当前活跃的 QUIC 连接数",
		},
		[]string{"component"}, // component: dispatcher, client
	)

	// WebSocketConnectionErrors 统计 WebSocket 连接错误数，按组件和错误类型分类
	// 使用场景：在 gateway 和 client 中，处理 WebSocket 错误（如 accept, read, write, close）时调用 Inc()
	// 示例：WebSocketConnectionErrors.WithLabelValues("gateway", "read").Inc()
	WebSocketConnectionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_websocket_connection_errors_total",
			Help: "WebSocket 连接错误总数",
		},
		[]string{"component", "error_type"}, // error_type: accept, read, write, close
	)

	// QUICConnectionErrors 统计 QUIC 连接错误数，按组件和错误类型分类
	// 使用场景：在 dispatcher 和 client 中，处理 QUIC 错误（如 accept, read, write, close, open_stream）时调用 Inc()
	// 示例：QUICConnectionErrors.WithLabelValues("dispatcher", "accept").Inc()
	QUICConnectionErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_quic_connection_errors_total",
			Help: "QUIC 连接错误总数",
		},
		[]string{"component", "error_type"}, // error_type: accept, read, write, close, open_stream
	)

	// BulletMessagesProcessed 跟踪处理的弹幕消息总数，按组件和状态分类
	// 使用场景：在 gateway, worker, dispatcher 中，处理弹幕消息成功或失败时调用 Inc()
	// 示例：BulletMessagesProcessed.WithLabelValues("worker", "success").Inc()
	BulletMessagesProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_bullet_messages_processed_total",
			Help: "处理的弹幕消息总数",
		},
		[]string{"component", "status"}, // status: success, failed
	)

	// BulletMessageLatency 测量弹幕消息处理延迟（秒），按组件和消息类型分类
	// 使用场景：在 gateway, worker, dispatcher 中，记录弹幕处理时间时调用 WithLabelValues().Observe()
	// 示例：BulletMessageLatency.WithLabelValues("gateway", "bullet").Observe(duration.Seconds())
	BulletMessageLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_bullet_message_latency_seconds",
			Help:    "弹幕消息处理延迟（秒）",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0}, // 优化桶以捕获 100ms 目标
		},
		[]string{"component", "message_type"}, // message_type: bullet, heartbeat, create_room, check_room
	)

	// KafkaMessagesSent 统计发送到 Kafka 的消息数，按状态分类
	// 使用场景：在 gateway 中，发送 Kafka 消息成功或失败时调用 Inc()
	// 示例：KafkaMessagesSent.WithLabelValues("success").Inc()
	KafkaMessagesSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_kafka_messages_sent_total",
			Help: "发送到 Kafka 的消息总数",
		},
		[]string{"status"}, // status: success, failed
	)

	// KafkaMessageLatency 测量 Kafka 消息写入延迟（秒）
	// 使用场景：在 gateway 中，记录 Kafka 写入时间时调用 WithLabelValues().Observe()
	// 示例：KafkaMessageLatency.WithLabelValues().Observe(duration.Seconds())
	KafkaMessageLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_kafka_message_latency_seconds",
			Help:    "Kafka 消息写入延迟（秒）",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5}, // 关注低延迟场景
		},
		[]string{}, // 无标签
	)

	// RedisOperations 统计 Redis 操作数，按操作类型和状态分类
	// 使用场景：在 gateway, worker, dispatcher 中，执行 Redis 操作（如 sadd, srem, zadd）时调用 Inc()
	// 示例：RedisOperations.WithLabelValues("sadd", "success").Inc()
	RedisOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_redis_operations_total",
			Help: "Redis 操作总数",
		},
		[]string{"operation", "status"}, // operation: sadd, srem, zadd, zrange, throttle; status: success, failed
	)

	// RedisOperationLatency 测量 Redis 操作延迟（秒），按操作类型分类
	// 使用场景：在 gateway, worker, dispatcher 中，记录 Redis 操作时间时调用 WithLabelValues().Observe()
	// 示例：RedisOperationLatency.WithLabelValues("sadd").Observe(duration.Seconds())
	RedisOperationLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "edulive_redis_operation_latency_seconds",
			Help:    "Redis 操作延迟（秒）",
			Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1}, // Redis 操作通常很快
		},
		[]string{"operation"},
	)

	// BulletFiltered 统计被过滤的弹幕数，按过滤类型分类
	// 使用场景：在 worker 中，弹幕被过滤（如 sensitive_word, duplicate, rate_limit, low_level）时调用 Inc()
	// 示例：BulletFiltered.WithLabelValues("sensitive_word").Inc()
	BulletFiltered = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_bullet_filtered_total",
			Help: "被过滤的弹幕总数",
		},
		[]string{"filter_type"}, // filter_type: sensitive_word, duplicate, rate_limit, low_level
	)

	// LiveRoomCreations 统计直播间创建请求数，按状态分类
	// 使用场景：在 gateway 中，处理创建直播间请求时调用 Inc()
	// 示例：LiveRoomCreations.WithLabelValues("success").Inc()
	LiveRoomCreations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_live_room_creations_total",
			Help: "直播间创建请求总数",
		},
		[]string{"status"}, // status: success, failed
	)

	// LiveRoomChecks 统计直播间检查请求数，按状态分类
	// 使用场景：在 gateway 中，处理检查直播间请求时调用 Inc()
	// 示例：LiveRoomChecks.WithLabelValues("exists").Inc()
	LiveRoomChecks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_live_room_checks_total",
			Help: "直播间检查请求总数",
		},
		[]string{"status"}, // status: exists, not_exists, failed
	)

	// RankingUpdates 统计排行榜更新操作数，按状态分类
	// 使用场景：在 worker 中，更新排行榜（如 ZIncrBy）时调用 Inc()
	// 示例：RankingUpdates.WithLabelValues("success").Inc()
	RankingUpdates = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_ranking_updates_total",
			Help: "排行榜更新操作总数",
		},
		[]string{"status"}, // status: success, failed
	)

	// PoolUsage 跟踪对象池的当前使用量，按池类型分类
	// 使用场景：在 pkg/pool/pool.go 中，Get 时调用 Inc()，Put 时调用 Dec()
	// 示例：PoolUsage.WithLabelValues("bullet_message").Inc()
	PoolUsage = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "edulive_pool_usage",
			Help: "对象池当前使用量",
		},
		[]string{"pool_type"}, // pool_type: bullet_message, bytes_buffer, kafka_key, bullet_slice, serialized_bullet
	)

	// MemoryPoolAllocations 统计内存池分配次数，按池类型分类
	// 使用场景：在 pkg/pool/pool.go 中，Get 时调用 Inc()
	// 示例：MemoryPoolAllocations.WithLabelValues("bullet_message").Inc()
	MemoryPoolAllocations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_memory_pool_allocations_total",
			Help: "内存池分配次数",
		},
		[]string{"pool_type"},
	)

	// CompressionRatio 测量弹幕消息压缩比率（压缩后大小/原始大小）
	// 使用场景：在 pkg/protocol/protocol.go 中，Encode 时调用 Observe()
	// 示例：CompressionRatio.Observe(float64(compressedSize) / float64(originalSize))
	CompressionRatio = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "edulive_bullet_compression_ratio",
			Help:    "弹幕消息压缩比率（压缩后大小/原始大小）",
			Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0},
		},
	)

	// ProtocolParseErrors 统计协议解析错误数，按消息类型分类
	// 使用场景：在 gateway, client 中，Decode 或 Encode 失败时调用 Inc()
	// 示例：ProtocolParseErrors.WithLabelValues("bullet").Inc()
	ProtocolParseErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_protocol_parse_errors_total",
			Help: "协议解析错误总数",
		},
		[]string{"message_type"}, // message_type: bullet, heartbeat, create_room, check_room
	)

	// RateLimitRejections 统计因频率限制拒绝的弹幕数，按用户ID分类
	// 使用场景：在 worker 中，频率限制（如 CL.THROTTLE）拒绝弹幕时调用 Inc()
	// 示例：RateLimitRejections.WithLabelValues("12345").Inc()
	RateLimitRejections = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_rate_limit_rejections_total",
			Help: "因频率限制拒绝的弹幕总数",
		},
		[]string{"user_id"},
	)

	// HeartbeatFailures 统计心跳消息失败数，按组件分类
	// 使用场景：在 client 中，心跳发送失败时调用 Inc()
	// 示例：HeartbeatFailures.WithLabelValues("client").Inc()
	HeartbeatFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "edulive_heartbeat_failures_total",
			Help: "心跳消息失败总数",
		},
		[]string{"component"},
	)

	WebSocketMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_messages_received_total",
			Help: "Total number of WebSocket messages received",
		},
		[]string{"component"},
	)

	WebSocketMessagesDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "websocket_messages_dropped_total",
			Help: "Total number of WebSocket messages dropped",
		},
		[]string{"component"},
	)

	KafkaMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_received_total",
			Help: "Total number of Kafka messages received",
		},
		[]string{"component"},
	)
	KafkaMessagesDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_dropped_total",
			Help: "Total number of Kafka messages dropped due to channel full",
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

	// 可添加额外的初始化逻辑，例如设置默认标签或注册自定义收集器
	// 示例：为 PoolUsage 注册 GaugeFunc 动态计算池大小（可选）
	/*
		prometheus.Register(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: "edulive_pool_size",
				Help: "对象池当前对象总数",
			},
			func() float64 {
				// 假设有方法获取池大小
				return float64(getPoolSize("bullet_message"))
			},
		))
	*/
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
	WebSocketMessagesReceived.Reset()
	WebSocketMessagesDropped.Reset()
	KafkaMessagesReceived.Reset()
	KafkaMessagesDropped.Reset()
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

// RecordLatency 记录操作延迟并更新相应的 Histogram 指标
func RecordLatency(ctx context.Context, operation string, duration time.Duration, labels ...string) {
	span := trace.SpanFromContext(ctx)
	span.AddEvent("latency", trace.WithAttributes(
		attribute.String("operation", operation),
		attribute.Float64("duration_seconds", duration.Seconds()),
	))

	// 根据操作类型更新相应的延迟指标
	switch {
	case strings.HasPrefix(operation, "websocket"):
		if len(labels) < 1 {
			labels = []string{"gateway", "bullet"}
		}
		BulletMessageLatency.WithLabelValues(labels...).Observe(duration.Seconds())
	case strings.HasPrefix(operation, "quic"):
		if len(labels) < 1 {
			labels = []string{"dispatcher", "bullet"}
		}
		BulletMessageLatency.WithLabelValues(labels...).Observe(duration.Seconds())
	case strings.HasPrefix(operation, "kafka"):
		KafkaMessageLatency.WithLabelValues().Observe(duration.Seconds())
	case strings.HasPrefix(operation, "redis"):
		if len(labels) < 1 {
			labels = []string{"generic"}
		}
		RedisOperationLatency.WithLabelValues(labels[0]).Observe(duration.Seconds())
	}
}

// RecordError 记录错误并更新相应的 Counter 指标
func RecordError(span trace.Span, err error, operation string, labels ...string) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	// 根据操作类型更新相应的错误指标
	switch {
	case strings.Contains(operation, "websocket"):
		if len(labels) < 2 {
			labels = []string{"gateway", "generic"}
		}
		WebSocketConnectionErrors.WithLabelValues(labels...).Inc()
	case strings.Contains(operation, "quic"):
		if len(labels) < 2 {
			labels = []string{"dispatcher", "generic"}
		}
		QUICConnectionErrors.WithLabelValues(labels...).Inc()
	case strings.Contains(operation, "kafka"):
		if len(labels) < 1 {
			labels = []string{"failed"}
		}
		KafkaMessagesSent.WithLabelValues(labels[0]).Inc()
	case strings.Contains(operation, "redis"):
		if len(labels) < 2 {
			labels = []string{"generic", "failed"}
		}
		RedisOperations.WithLabelValues(labels...).Inc()
	case strings.Contains(operation, "protocol"):
		if len(labels) < 1 {
			labels = []string{"bullet"}
		}
		ProtocolParseErrors.WithLabelValues(labels[0]).Inc()
	}
}
