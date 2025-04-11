package observability

import (
	"context"
	"fmt"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// tracer 是全局的 Tracer 实例
var tracer trace.Tracer

// Metrics
var (
	operationLatency metric.Float64Histogram
	errorCounter     metric.Int64Counter
)

// InitTracing 初始化分布式追踪和指标
func InitTracing(cfg *config.Config) error {
	// 初始化 Jaeger 导出器
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint(cfg.Observability.Jaeger.HttpEndpoint),
		otlptracehttp.WithInsecure(), // 开发环境使用
	)
	if err != nil {
		logger.Error("Failed to create OTLP exporter", zap.Error(err))
		return fmt.Errorf("create OTLP exporter: %w", err)
	}

	// 定义服务资源
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", cfg.Type),
			attribute.String("service.version", "0.1.0"),
			attribute.String("environment", "production"),
		),
	)
	if err != nil {
		logger.Error("Failed to create resource", zap.Error(err))
		return fmt.Errorf("create resource: %w", err)
	}

	// 初始化 Tracer 提供者
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Observability.Jaeger.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// 初始化全局 Tracer
	tracer = otel.Tracer("edulive")

	// 初始化 Prometheus 导出器
	if cfg.Observability.Prometheus.Enabled {
		promExporter, err := prometheus.New()
		if err != nil {
			logger.Error("Failed to create Prometheus exporter", zap.Error(err))
			return fmt.Errorf("create Prometheus exporter: %w", err)
		}
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(promExporter))
		otel.SetMeterProvider(provider)

		// 初始化指标
		meter := provider.Meter("edulive")
		operationLatency, err = meter.Float64Histogram(
			"operation_latency_seconds",
			metric.WithDescription("Latency of operations in seconds"),
			metric.WithUnit("s"),
		)
		if err != nil {
			logger.Error("Failed to create latency histogram", zap.Error(err))
			return fmt.Errorf("create latency histogram: %w", err)
		}
		errorCounter, err = meter.Int64Counter(
			"operation_errors_total",
			metric.WithDescription("Total number of operation errors"),
		)
		if err != nil {
			logger.Error("Failed to create error counter", zap.Error(err))
			return fmt.Errorf("create error counter: %w", err)
		}
	}

	logger.Info("Tracing initialized",
		zap.String("jaeger_endpoint", cfg.Observability.Jaeger.HttpEndpoint),
		zap.Bool("prometheus_enabled", cfg.Observability.Prometheus.Enabled))
	return nil
}

// StartSpan 创建并返回一个新的 Span
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, opts...)
}
