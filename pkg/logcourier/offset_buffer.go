package logcourier

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// FlushReason indicates why a flush operation was triggered
type FlushReason string

// FlushReason values describe the trigger for an offset flush operation
const (
	FlushReasonTimeThreshold  FlushReason = "time threshold"
	FlushReasonCountThreshold FlushReason = "count threshold"
	FlushReasonCycleBoundary  FlushReason = "cycle boundary"
	FlushReasonShutdown       FlushReason = "shutdown"
	FlushReasonExplicit       FlushReason = "explicit"
)

// offsetCommitRequest represents a request to commit a single offset
type offsetCommitRequest struct {
	offset Offset
	key    offsetKey
}

// flushRequest represents a request to flush buffered offsets
type flushRequest struct {
	response chan error
	ctx      context.Context
	reason   FlushReason
}

// OffsetBuffer buffers offsets in memory and flushes them on demand.
//
//nolint:govet // fieldalignment: struct field order optimized for readability over memory alignment
type OffsetBuffer struct {
	// Coordination channels
	offsetCh   chan offsetCommitRequest
	flushReqCh chan flushRequest

	// Primary storage: (bucket, raftSessionID) -> latest offset
	// Accessed only by flushLoop goroutine
	offsets map[offsetKey]Offset

	offsetManager OffsetManagerInterface
	logger        *slog.Logger

	// Configuration
	initialBackoff      time.Duration
	maxBackoff          time.Duration
	flushTimeThreshold  time.Duration
	backoffJitterFactor float64

	maxRetries          int
	flushCountThreshold int
}

type offsetKey struct {
	bucket        string
	raftSessionID uint16
}

// OffsetBufferOptions holds configuration and dependencies for OffsetBuffer
type OffsetBufferOptions struct {
	OffsetManager       OffsetManagerInterface
	Logger              *slog.Logger
	InitialBackoff      time.Duration
	MaxBackoff          time.Duration
	BackoffJitterFactor float64
	MaxRetries          int
	FlushTimeThreshold  time.Duration
	FlushCountThreshold int
	NumWorkers          int
}

// NewOffsetBuffer creates a new offset buffer
func NewOffsetBuffer(cfg OffsetBufferOptions) *OffsetBuffer {
	// Buffer size allows workers to queue offset commits while a flush is in progress.
	// This prevents deadlock when Put() is called during a flush operation.
	bufferSize := cfg.NumWorkers
	if bufferSize == 0 {
		bufferSize = 10 // Default buffer size for tests and single-worker scenarios
	}

	return &OffsetBuffer{
		offsetCh:            make(chan offsetCommitRequest, bufferSize),
		flushReqCh:          make(chan flushRequest),
		offsets:             make(map[offsetKey]Offset),
		maxRetries:          cfg.MaxRetries,
		initialBackoff:      cfg.InitialBackoff,
		maxBackoff:          cfg.MaxBackoff,
		backoffJitterFactor: cfg.BackoffJitterFactor,
		offsetManager:       cfg.OffsetManager,
		logger:              cfg.Logger,
		flushTimeThreshold:  cfg.FlushTimeThreshold,
		flushCountThreshold: cfg.FlushCountThreshold,
	}
}

// Start begins the flush loop in a background goroutine.
// Returns a stop function that cancels the loop and waits for it to complete.
func (ob *OffsetBuffer) Start(ctx context.Context) func() {
	// Create child context so we can cancel independently
	childCtx, cancel := context.WithCancel(ctx)

	// Track when goroutine exits
	done := make(chan struct{})

	go func() {
		ob.flushLoop(childCtx)
		close(done)
	}()

	// Return stop function
	return func() {
		cancel()
		<-done
	}
}

// drainPendingCommits receives all pending commits from offsetCh (non-blocking)
// This ensures that when Flush() is called, all buffered Put() calls are processed first
func (ob *OffsetBuffer) drainPendingCommits() {
	for {
		select {
		case commit := <-ob.offsetCh:
			ob.offsets[commit.key] = commit.offset
		default:
			// No more pending commits
			return
		}
	}
}

// flushLoop is the goroutine that manages all offset operations.
// It receives offsets from Put(), handles time-based and count-based flush triggers,
// and processes explicit flush requests.
func (ob *OffsetBuffer) flushLoop(ctx context.Context) {
	var ticker *time.Ticker
	var tickerCh <-chan time.Time

	if ob.flushTimeThreshold > 0 {
		ticker = time.NewTicker(ob.flushTimeThreshold)
		defer ticker.Stop()
		tickerCh = ticker.C
	}

	for {
		select {
		case commit := <-ob.offsetCh:
			ob.offsets[commit.key] = commit.offset

			// Check count threshold
			if ob.flushCountThreshold > 0 && len(ob.offsets) >= ob.flushCountThreshold {
				if err := ob.performFlush(ctx, FlushReasonCountThreshold); err != nil {
					ob.logger.Error("count-based flush failed", "error", err)
				}
			}

		case <-tickerCh:
			// Time-based flush
			if err := ob.performFlush(ctx, FlushReasonTimeThreshold); err != nil {
				ob.logger.Error("time-based flush failed", "error", err)
			}

		case req := <-ob.flushReqCh:
			// Explicit flush request: first drain all pending commits from offsetCh
			// to ensure we flush the most up-to-date state
			ob.drainPendingCommits()

			err := ob.performFlush(req.ctx, req.reason)
			req.response <- err

		case <-ctx.Done():
			// Shutdown: drain pending commits and flush remaining offsets
			ob.drainPendingCommits()

			shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := ob.performFlush(shutdownCtx, FlushReasonShutdown); err != nil {
				ob.logger.Error("shutdown flush failed", "error", err)
			}
			cancel()
			return
		}
	}
}

// performFlush flushes all currently buffered offsets
func (ob *OffsetBuffer) performFlush(ctx context.Context, reason FlushReason) error {
	if len(ob.offsets) == 0 {
		ob.logger.Debug("flush skipped, buffer empty", "reason", reason)
		return nil
	}

	offsetCount := len(ob.offsets)

	if err := ob.flushBatch(ctx, ob.offsets); err != nil {
		ob.logger.Error("offset flush failed",
			"reason", reason,
			"bufferedOffsets", offsetCount,
			"error", err)
		return err
	}

	// Clear successfully flushed offsets
	clear(ob.offsets)

	ob.logger.Info("flushed offsets",
		"reason", reason,
		"offsetCount", offsetCount)

	return nil
}

// Put stores an offset in the buffer.
// This method is safe to call from multiple goroutines.
func (ob *OffsetBuffer) Put(bucket string, raftSessionID uint16, offset Offset) {
	commit := offsetCommitRequest{
		offset: offset,
		key:    offsetKey{bucket: bucket, raftSessionID: raftSessionID},
	}

	// Send to flushLoop goroutine
	ob.offsetCh <- commit
}

// Flush commits all buffered offsets to ClickHouse.
// This method blocks until the flush completes or ctx is cancelled.
// It is safe to call from multiple goroutines.
func (ob *OffsetBuffer) Flush(ctx context.Context, reason FlushReason) error {
	req := flushRequest{
		response: make(chan error, 1),
		ctx:      ctx,
		reason:   reason,
	}

	// Send flush request to flushLoop
	select {
	case ob.flushReqCh <- req:
		// Wait for response
		select {
		case err := <-req.response:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// flushBatch commits a batch of offsets to ClickHouse with retry logic
func (ob *OffsetBuffer) flushBatch(ctx context.Context, offsets map[offsetKey]Offset) error {
	var lastErr error
	backoff := ob.initialBackoff

	for attempt := 0; attempt <= ob.maxRetries; attempt++ {
		if attempt > 0 {
			actualBackoff := applyJitter(backoff, ob.backoffJitterFactor)

			ob.logger.Info("retrying offset flush after backoff",
				"attempt", attempt,
				"backoffSeconds", actualBackoff.Seconds())

			select {
			case <-time.After(actualBackoff):
				// Backoff period elapsed, proceed with retry
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
	commits := make([]OffsetCommit, 0, len(offsets))
	for key, offset := range offsets {
		commits = append(commits, OffsetCommit{
			Offset:        offset,
			Bucket:        key.bucket,
			RaftSessionID: key.raftSessionID,
		})
	}

	return ob.offsetManager.CommitOffsetsBatch(ctx, commits)
}
