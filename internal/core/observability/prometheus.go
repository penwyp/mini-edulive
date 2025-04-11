package observability

import (
	"net/http"

	"github.com/penwyp/mini-edulive/config"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func TryEnablePrometheusExport(cfg *config.Config) {
	// Start Prometheus metrics endpoint if enabled
	if cfg.Observability.Prometheus.Enabled {
		metricsPort := cfg.Observability.GetPrometheusExportPortStr()
		go func() {
			http.Handle(cfg.Observability.Prometheus.Path, promhttp.Handler())
			logger.Info("Starting Prometheus metrics endpoint", zap.String("port", metricsPort))
			if err := http.ListenAndServe(":"+metricsPort, nil); err != nil {
				logger.Panic("Failed to start Prometheus metrics endpoint", zap.Error(err))
			}
		}()
	}
}
