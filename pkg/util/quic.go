package util

import (
	"crypto/tls"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"go.uber.org/zap"
)

func GenerateTLSConfig(certFile, keyFile string) *tls.Config {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		logger.Panic("Failed to load TLS cert", zap.Error(err))
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-edulive"},
	}
}
