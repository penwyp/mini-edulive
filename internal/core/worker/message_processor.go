package worker

import (
	"context"
	"errors"

	"github.com/penwyp/mini-edulive/internal/core/observability"
	"github.com/penwyp/mini-edulive/pkg/logger"
	"github.com/penwyp/mini-edulive/pkg/protocol"
	"go.uber.org/zap"
)

var (
	ErrMessageRejected = errors.New("message rejected by filter")
	ErrStorageFailed   = errors.New("failed to store message")
)

// MessageProcessor handles the processing flow for bullet messages
type MessageProcessor struct {
	filterChain *FilterChain
	storage     *BulletStorage
}

// NewMessageProcessor creates a new message processor
func NewMessageProcessor(filterChain *FilterChain, storage *BulletStorage) *MessageProcessor {
	return &MessageProcessor{
		filterChain: filterChain,
		storage:     storage,
	}
}

// Process runs a message through the filter chain and stores it if approved
func (p *MessageProcessor) Process(ctx context.Context, bullet *protocol.BulletMessage) error {
	ctx, span := observability.StartSpan(ctx, "MessageProcessor.Process")
	defer span.End()

	// Apply filters
	if passed, reason := p.filterChain.Apply(ctx, bullet); !passed {
		logger.Debug("Message rejected by filter",
			zap.String("reason", reason),
			zap.Uint64("liveID", bullet.LiveID),
			zap.Uint64("userID", bullet.UserID))
		return ErrMessageRejected
	}

	// Store the message
	if err := p.storage.Store(ctx, bullet); err != nil {
		logger.Error("Failed to store message",
			zap.Error(err),
			zap.Uint64("liveID", bullet.LiveID),
			zap.Uint64("userID", bullet.UserID))
		return ErrStorageFailed
	}

	return nil
}
