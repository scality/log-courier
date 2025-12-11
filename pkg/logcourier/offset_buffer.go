package logcourier

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// OffsetBuffer buffers offsets in memory and flushes them on demand
type OffsetBuffer struct {
	// Primary storage: (bucket, raftSessionID) -> latest offset
	offsets map[offsetKey]Offset

	offsetManager OffsetManagerInterface
	logger        *slog.Logger

	// Configuration
	initialBackoff      time.Duration
	maxBackoff          time.Duration
	backoffJitterFactor float64
	maxRetries          int

	mu sync.Mutex
}

type offsetKey struct {
	bucket        string
	raftSessionID uint16
}

// OffsetBufferConfig holds configuration for OffsetBuffer
type OffsetBufferConfig struct {
	OffsetManager       OffsetManagerInterface
	Logger              *slog.Logger
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	BackoffJitterFactor float64
	MaxRetries          int
}

// NewOffsetBuffer creates a new offset buffer
func NewOffsetBuffer(cfg OffsetBufferConfig) *OffsetBuffer {
	return &OffsetBuffer{
		offsets:             make(map[offsetKey]Offset),
		maxRetries:          cfg.MaxRetries,
		initialBackoff:      cfg.InitialBackoff,
		maxBackoff:          cfg.MaxBackoff,
		backoffJitterFactor: cfg.BackoffJitterFactor,
		offsetManager:       cfg.OffsetManager,
		logger:              cfg.Logger,
	}
}

// Put stores an offset in the buffer
func (ob *OffsetBuffer) Put(bucket string, raftSessionID uint16, offset Offset) {
	ob.mu.Lock()
	defer ob.mu.Unlock()

	key := offsetKey{bucket: bucket, raftSessionID: raftSessionID}
	ob.offsets[key] = offset
}

// Flush commits all buffered offsets to ClickHouse
func (ob *OffsetBuffer) Flush(ctx context.Context) error {
	ob.mu.Lock()
	if len(ob.offsets) == 0 {
		ob.mu.Unlock()
		return nil
	}

	// Take snapshot of offsets to flush
	offsetsToFlush := make(map[offsetKey]Offset, len(ob.offsets))
	for k, v := range ob.offsets {
		offsetsToFlush[k] = v
	}
	ob.mu.Unlock()

	// Flush without holding lock
	if err := ob.flushBatch(ctx, offsetsToFlush); err != nil {
		return err
	}

	// Clear successfully flushed offsets
	ob.mu.Lock()
	defer ob.mu.Unlock()

	for key := range offsetsToFlush {
		delete(ob.offsets, key)
	}

	ob.logger.Info("flushed offsets", "count", len(offsetsToFlush))

	return nil
}

// flushBatch commits a batch of offsets to ClickHouse with retry logic
func (ob *OffsetBuffer) flushBatch(ctx context.Context, offsets map[offsetKey]Offset) error {
	var lastErr error
	backoff := ob.initialBackoff

	for attempt := 0; attempt <= ob.maxRetries; attempt++ {
		if attempt > 0 {
			// Apply jitter to the backoff
			actualBackoff := applyJitter(backoff, ob.backoffJitterFactor)

			ob.logger.Info("retrying offset flush after backoff",
				"attempt", attempt,
				"backoffSeconds", actualBackoff.Seconds())

			select {
			case <-time.After(actualBackoff):
			case <-ctx.Done():
				return ctx.Err()
			}

			backoff = time.Duration(float64(backoff) * 2.0)
			if backoff > ob.maxBackoff {
				backoff = ob.maxBackoff
			}
		}

		err := ob.commitBatch(ctx, offsets)
		if err == nil {
			if attempt > 0 {
				ob.logger.Info("offset flush succeeded after retries", "attempt", attempt)
			}
			return nil
		}

		lastErr = err
		ob.logger.Warn("transient error, will retry offset flush",
			"attempt", attempt,
			"error", err)
	}

	return fmt.Errorf("max retries (%d) exceeded for offset flush: %w", ob.maxRetries, lastErr)
}

// commitBatch performs the actual batch commit
func (ob *OffsetBuffer) commitBatch(ctx context.Context, offsets map[offsetKey]Offset) error {
	// Convert map to slice of requests
	requests := make([]OffsetCommitRequest, 0, len(offsets))
	for key, offset := range offsets {
		requests = append(requests, OffsetCommitRequest{
			Offset:        offset,
			Bucket:        key.bucket,
			RaftSessionID: key.raftSessionID,
		})
	}

	return ob.offsetManager.CommitOffsetsBatch(ctx, requests)
}
