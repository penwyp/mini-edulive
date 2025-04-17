package util

import (
	"crypto/tls"
	"log"

	"go.uber.org/zap"
)

func GenerateTLSConfig(certFile, keyFile string) *tls.Config {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Panic("Failed to load TLS cert", zap.Error(err))
	}
	return &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
		NextProtos:         []string{"quic-edulive"},
	}
}
