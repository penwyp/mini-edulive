package observability

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/golang/protobuf/proto" // 使用旧版 Protobuf
	"github.com/golang/snappy"
	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/prometheus/client_golang/prometheus"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

var (
	oncePrometheus sync.Once
)

// TryEnablePrometheusExport 启动 Prometheus 指标暴露（仅限 gateway/worker/dispatcher）
func TryEnablePrometheusExport(cfg *config.Config) {
	if cfg.Type == "client" {
		logger.Info("Prometheus export skipped for client, using remote write instead")
		return
	}

	if !cfg.Observability.Prometheus.Enabled {
		logger.Info("Prometheus export is disabled")
		return
	}

	oncePrometheus.Do(func() {
		port := cfg.Observability.GetPrometheusExportPortStr()
		logger.Info("Starting Prometheus metrics endpoint", zap.String("port", port))
		go func() {
			http.Handle(cfg.Observability.Prometheus.Path, promhttp.Handler())
			if err := http.ListenAndServe(":"+port, nil); err != nil {
				logger.Error("Failed to start Prometheus metrics endpoint", zap.Error(err))
			}
		}()
	})
}

// StartPrometheusRemoteWrite 启动 Prometheus 远程写入（仅限客户端）
func StartPrometheusRemoteWrite(ctx context.Context, cfg *config.Config, wg *sync.WaitGroup) {
	if cfg.Type != "client" || !cfg.Observability.Prometheus.Enabled {
		logger.Info("Prometheus remote write disabled or not applicable", zap.String("type", cfg.Type))
		return
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(15 * time.Second) // 每 15 秒推送一次
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// 推送最终指标并退出
				if err := pushMetrics(client, cfg); err != nil {
					logger.Error("Final Prometheus remote write failed", zap.Error(err))
				} else {
					logger.Info("Final Prometheus remote write completed")
				}
				return
			case <-ticker.C:
				if err := pushMetrics(client, cfg); err != nil {
					logger.Error("Prometheus remote write failed", zap.Error(err))
				} else {
					logger.Debug("Prometheus remote write successful")
				}
			}
		}
	}()
}

// pushMetrics 推送指标到 Prometheus
func pushMetrics(client *http.Client, cfg *config.Config) error {
	// 收集指标
	metricFamilies, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics: %w", err)
	}

	// 构造 WriteRequest
	writeRequest := &prompb.WriteRequest{
		Timeseries: make([]prompb.TimeSeries, 0),
	}

	for _, mf := range metricFamilies {
		for _, m := range mf.Metric {
			// 构造标签
			labels := []prompb.Label{
				{Name: "__name__", Value: *mf.Name},
				{Name: "user_id", Value: fmt.Sprintf("%d", cfg.Client.UserID)},
				{Name: "live_id", Value: fmt.Sprintf("%d", cfg.Client.LiveID)},
			}
			for _, l := range m.Label {
				if l.Name != nil && l.Value != nil {
					labels = append(labels, prompb.Label{
						Name:  *l.Name,
						Value: *l.Value,
					})
				}
			}

			// 设置时间戳
			timestamp := time.Now().UnixMilli()
			if m.TimestampMs != nil {
				timestamp = *m.TimestampMs
			}

			// 构造样本
			samples := []prompb.Sample{}
			switch *mf.Type {
			case io_prometheus_client.MetricType_COUNTER:
				if m.Counter != nil && m.Counter.Value != nil {
					samples = append(samples, prompb.Sample{
						Value:     *m.Counter.Value,
						Timestamp: timestamp,
					})
				}
			case io_prometheus_client.MetricType_GAUGE:
				if m.Gauge != nil && m.Gauge.Value != nil {
					samples = append(samples, prompb.Sample{
						Value:     *m.Gauge.Value,
						Timestamp: timestamp,
					})
				}
				// 可根据需要添加 Histogram 或 Summary 支持
			}

			if len(samples) > 0 {
				writeRequest.Timeseries = append(writeRequest.Timeseries, prompb.TimeSeries{
					Labels:  labels,
					Samples: samples,
				})
			}
		}
	}

	// 记录推送的指标数量
	logger.Debug("Preparing to push metrics", zap.Int("timeseries_count", len(writeRequest.Timeseries)))

	// 序列化为 Protobuf
	data, err := proto.Marshal(writeRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal write request: %w", err)
	}

	// 使用 Snappy 压缩
	compressed := snappy.Encode(nil, data)

	// 发送 HTTP 请求
	req, err := http.NewRequest("POST", cfg.Observability.Prometheus.RemoteWrite, bytes.NewReader(compressed))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// 接受 204（No Content）或 200（OK）作为成功响应
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	logger.Debug("Prometheus remote write response", zap.Int("status_code", resp.StatusCode))
	return nil
}
