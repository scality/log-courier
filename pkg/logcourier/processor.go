package logcourier

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/scality/log-courier/pkg/clickhouse"
	"github.com/scality/log-courier/pkg/s3"
)

const (
	// backoffMultiplier is the exponential backoff multiplier for retry attempts
	backoffMultiplier = 2.0

	// maxConsecutiveCycleFailures is the maximum number of consecutive cycle failures
	// before the processor exits. This prevents infinite error loops when there's a
	// systemic issue (e.g., ClickHouse permanently down, invalid credentials).
	maxConsecutiveCycleFailures = 3
)

// Processor is the main log courier processor
type Processor struct {
	// Components
	clickhouseClient *clickhouse.Client
	s3Uploader       s3.UploaderInterface
	workDiscovery    *BatchFinder
	logFetcher       *LogFetcher
	logBuilder       *LogObjectBuilder
	offsetManager    OffsetManagerInterface
	logger           *slog.Logger

	// Configuration
	discoveryInterval time.Duration
	numWorkers        int
	maxRetries        int
	initialBackoff    time.Duration
	maxBackoff        time.Duration

	// State
	cycleRunning         bool // Tracks if a cycle is currently in progress
	consecutiveFailures  int  // Tracks consecutive cycle failures
}

// Config holds processor configuration
//nolint:govet // Field alignment is less important than readability for config structs
type Config struct {
	Logger *slog.Logger

	ClickHouseTimeout time.Duration
	// DiscoveryInterval is the interval between work discovery runs
	DiscoveryInterval time.Duration

	// CountThreshold is the minimum number of unprocessed logs required to trigger batch processing
	CountThreshold int
	// TimeThresholdSec is the age in seconds after which logs should be processed regardless of count
	TimeThresholdSec int
	// NumWorkers is the number of parallel workers for batch processing
	NumWorkers int

	// MaxRetries is the maximum number of retry attempts for failed operations
	MaxRetries int
	// InitialBackoff is the initial backoff duration for retry attempts
	InitialBackoff time.Duration
	// MaxBackoff is the maximum backoff duration for retry attempts
	MaxBackoff time.Duration

	ClickHouseURL      string
	ClickHouseUsername string
	ClickHousePassword string
	ClickHouseDatabase string
	S3Endpoint         string
	S3AccessKeyID      string
	S3SecretAccessKey  string

	// S3Uploader is an optional S3 uploader for testing (if nil, one will be created)
	S3Uploader s3.UploaderInterface
	// OffsetManager is an optional offset manager for testing (if nil, one will be created)
	OffsetManager OffsetManagerInterface
}

// NewProcessor creates a new processor
func NewProcessor(ctx context.Context, cfg Config) (*Processor, error) {
	// Default to standard database name if not specified
	database := cfg.ClickHouseDatabase
	if database == "" {
		database = clickhouse.DatabaseName
	}

	// Create ClickHouse client
	chClient, err := clickhouse.NewClient(ctx, clickhouse.Config{
		URL:      cfg.ClickHouseURL,
		Username: cfg.ClickHouseUsername,
		Password: cfg.ClickHousePassword,
		Timeout:  cfg.ClickHouseTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// Use provided S3 uploader or create one
	var s3Uploader s3.UploaderInterface
	if cfg.S3Uploader != nil {
		s3Uploader = cfg.S3Uploader
	} else {
		// Create S3 client
		s3Client, err := s3.NewClient(ctx, s3.Config{
			Endpoint:        cfg.S3Endpoint,
			AccessKeyID:     cfg.S3AccessKeyID,
			SecretAccessKey: cfg.S3SecretAccessKey,
		})
		if err != nil {
			_ = chClient.Close()
			return nil, fmt.Errorf("failed to create S3 client: %w", err)
		}
		s3Uploader = s3.NewUploader(s3Client)
	}

	// Use provided offset manager or create one
	var offsetManager OffsetManagerInterface
	if cfg.OffsetManager != nil {
		offsetManager = cfg.OffsetManager
	} else {
		offsetManager = NewOffsetManager(chClient, database)
	}

	return &Processor{
		clickhouseClient:  chClient,
		s3Uploader:        s3Uploader,
		workDiscovery:     NewBatchFinder(chClient, database, cfg.CountThreshold, cfg.TimeThresholdSec),
		logFetcher:        NewLogFetcher(chClient, database),
		logBuilder:        NewLogObjectBuilder(),
		offsetManager:     offsetManager,
		discoveryInterval: cfg.DiscoveryInterval,
		numWorkers:        cfg.NumWorkers,
		maxRetries:        cfg.MaxRetries,
		initialBackoff:    cfg.InitialBackoff,
		maxBackoff:        cfg.MaxBackoff,
		logger:            cfg.Logger,
	}, nil
}

// Close closes the processor and releases resources
func (p *Processor) Close() error {
	if p.clickhouseClient != nil {
		return p.clickhouseClient.Close()
	}
	return nil
}

// Run runs the processor main loop
//
// Cycle Failure Semantics:
// When a cycle fails the processor:
// - Waits for the next discoveryInterval before retrying (eventual delivery)
// - Tracks consecutive failures and exits if threshold exceeded
//
func (p *Processor) Run(ctx context.Context) error {
	p.logger.Info("processor starting")

	ticker := time.NewTicker(p.discoveryInterval)
	defer ticker.Stop()

	if err := p.runCycle(ctx); err != nil {
		p.consecutiveFailures++
		p.logger.Error("cycle failed",
			"error", err,
			"consecutiveFailures", p.consecutiveFailures,
			"maxConsecutiveCycleFailures", maxConsecutiveCycleFailures)
	} else {
		p.consecutiveFailures = 0
	}

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("processor stopping")
			return ctx.Err()

		case <-ticker.C:
			if p.cycleRunning {
				p.logger.Info("previous cycle still running, skipping this interval")
				continue
			}

			if err := p.runCycle(ctx); err != nil {
				p.consecutiveFailures++
				p.logger.Error("cycle failed",
					"error", err,
					"consecutiveFailures", p.consecutiveFailures,
					"maxConsecutiveCycleFailures", maxConsecutiveCycleFailures)

				if p.consecutiveFailures >= maxConsecutiveCycleFailures {
					return fmt.Errorf("processor stopping after %d consecutive cycle failures", p.consecutiveFailures)
				}
			} else {
				p.consecutiveFailures = 0
			}
		}
	}
}

// runCycle executes a single discovery and processing cycle
func (p *Processor) runCycle(ctx context.Context) error {
	p.cycleRunning = true
	defer func() {
		p.cycleRunning = false
	}()

	if err := p.runBatchFinder(ctx); err != nil {
		return fmt.Errorf("batch finder failed: %w", err)
	}

	return nil
}

// runBatchFinder executes batch finder and processes log batches in parallel
func (p *Processor) runBatchFinder(ctx context.Context) error {
	// Phase 1: Work Discovery
	p.logger.Debug("starting batch finder")

	batches, err := p.workDiscovery.FindBatches(ctx)
	if err != nil {
		return fmt.Errorf("batch finder failed: %w", err)
	}

	p.logger.Info("batch finder completed", "nBatches", len(batches))

	if len(batches) == 0 {
		return nil
	}

	// Phase 2: Process log batches in parallel
	var mu sync.Mutex
	var successCount int
	var failedBatches []string
	var permanentErrorCount int
	var transientErrorCount int

	g, gctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, p.numWorkers)

	for _, batch := range batches {
		batch := batch

		g.Go(func() error {
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-gctx.Done():
				return gctx.Err()
			}

			if err := p.processLogBatchWithRetry(gctx, batch); err != nil {
				mu.Lock()
				failedBatches = append(failedBatches, batch.Bucket)
				if IsPermanentError(err) {
					permanentErrorCount++
				} else {
					transientErrorCount++
				}
				mu.Unlock()

				p.logger.Error("batch processing failed",
					"bucketName", batch.Bucket,
					"logCount", batch.LogCount,
					"error", err)
			} else {
				mu.Lock()
				successCount++
				mu.Unlock()
			}

			return nil // Never propagate to errgroup
		})
	}

	// Wait for all workers to complete. We ignore the error because:
	// each goroutine returns nil, so g.Wait() only errors on context cancellation.
	// Batch failures are tracked individually and summarized below
	_ = g.Wait()

	if len(failedBatches) > 0 {
		p.logger.Warn("batch processing completed with failures",
			"totalBatches", len(batches),
			"successful", successCount,
			"failed", len(failedBatches),
			"failedBuckets", failedBatches)
	} else {
		p.logger.Info("all batches processed successfully",
			"totalBatches", len(batches))
	}

	// Cycle failure decision based on success and error types:
	//
	// 1. At least one success (s3 upload and offset commit) -> cycle succeeds
	//    Even if other batches failed, forward progress was made
	//
	// 2. No successes, only permanent errors -> cycle succeeds
	//    Misconfigured buckets should not cause processor exit
	//
	// 3. No successes, any transient errors -> cycle fails
	//    Indicates system-wide issue (ClickHouse down, S3 throttled, etc.)
	//    After 3 consecutive cycle failures, processor exits.
	if successCount == 0 && transientErrorCount > 0 {
		return fmt.Errorf("all %d batches failed", len(batches))
	}

	return nil
}

// ============================================================================
// BATCH PROCESSING ERROR HANDLING SEMANTICS
// ============================================================================
//
// ## Processing in two phases
//
// 1. Upload: Fetch logs from ClickHouse -> build log object -> upload to S3
// 2. Offset commit: Record highest processed timestamp in ClickHouse
//
// ## Error Classification
// - Permanent: Issues that won't resolve with retries
// - Transient: Temporary issues that may resolve
//
// ## Retry Behavior
//
// Upload phase:
//   - Permanent error: Immediate failure, logs remain for next processor cycle
//   - Transient error: Retry with backoff up to maxRetries
//
// Offset commit phase:
//   - All errors are treated as transient
//   - Retry with backoff up to maxRetries
//   - Failure means upload succeeded but offset not recorded
//
// ## At-least-once delivery
//
// Logs are delivered at least once, possibly multiple times:
//   - Upload fails: No S3 object, logs reprocessed next processor cycle
//   - Upload succeeds, offset commit fails: S3 object exists, logs reprocessed next processing cycle -> duplicate S3 object
//   - Crash after upload: S3 object exists, logs reprocessed on restart next processing cycle -> duplicate S3 object
//
// ## Processor cycle failure policy
//
// A cycle succeeds if:
//   - At least one batch commits successfully, OR
//   - All failures are permanent errors (only misconfigured buckets)
//
// A cycle fails if:
//   - No batches commit successfully AND
//   - At least one failure is a transient error
//
// Rationale:
//   - Permanent errors (misconfigured buckets) should not block processor
//   - Transient errors with no successes indicate system issues (ClickHouse down, S3 unavailable)
//   - After 3 consecutive cycle failures, processor exits. Prevents unnecessary retries if no progress can be made.
// ============================================================================

func (p *Processor) processLogBatchWithRetry(ctx context.Context, batch LogBatch) error {
	_, maxInsertedAt, raftSessionID, err := p.uploadLogBatchWithRetry(ctx, batch)
	if err != nil {
		return err
	}

	return p.commitOffsetWithRetry(ctx, batch.Bucket, raftSessionID, maxInsertedAt)
}

// uploadLogBatchWithRetry handles fetching, building, and uploading with retries
// Returns the records and offset info needed for committing
func (p *Processor) uploadLogBatchWithRetry(ctx context.Context, batch LogBatch) ([]LogRecord, time.Time, uint16, error) {
	var lastErr error
	backoff := p.initialBackoff

	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			p.logger.Info("retrying upload after backoff",
				"bucketName", batch.Bucket,
				"attempt", attempt,
				"backoffSeconds", backoff.Seconds())

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return nil, time.Time{}, 0, ctx.Err()
			}

			backoff = time.Duration(float64(backoff) * backoffMultiplier)
			if backoff > p.maxBackoff {
				backoff = p.maxBackoff
			}
		}

		records, maxInsertedAt, raftSessionID, err := p.uploadLogBatch(ctx, batch)
		if err == nil {
			p.logger.Info("upload succeeded",
				"bucketName", batch.Bucket,
				"attempt", attempt)
			return records, maxInsertedAt, raftSessionID, nil
		}

		lastErr = err

		if IsPermanentError(err) {
			p.logger.Error("permanent error, not retrying",
				"bucketName", batch.Bucket,
				"error", err)
			return nil, time.Time{}, 0, fmt.Errorf("permanent error uploading batch: %w", err)
		}

		p.logger.Warn("transient error, will retry upload",
			"bucketName", batch.Bucket,
			"attempt", attempt,
			"error", err)
	}

	return nil, time.Time{}, 0, fmt.Errorf("max retries (%d) exceeded for upload: %w", p.maxRetries, lastErr)
}

// commitOffsetWithRetry handles committing the offset to ClickHouse with retries
func (p *Processor) commitOffsetWithRetry(ctx context.Context, bucket string, raftSessionID uint16, maxInsertedAt time.Time) error {
	var lastErr error
	backoff := p.initialBackoff

	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			p.logger.Info("retrying offset commit after backoff",
				"bucketName", bucket,
				"raftSessionID", raftSessionID,
				"attempt", attempt,
				"backoffSeconds", backoff.Seconds())

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}

			backoff = time.Duration(float64(backoff) * backoffMultiplier)
			if backoff > p.maxBackoff {
				backoff = p.maxBackoff
			}
		}

		err := p.offsetManager.CommitOffset(ctx, bucket, raftSessionID, maxInsertedAt)
		if err == nil {
			p.logger.Info("offset commit succeeded",
				"bucketName", bucket,
				"raftSessionID", raftSessionID,
				"attempt", attempt,
				"maxInsertedAt", maxInsertedAt)
			return nil
		}

		lastErr = err
		p.logger.Warn("transient error, will retry offset commit",
			"bucketName", bucket,
			"raftSessionID", raftSessionID,
			"attempt", attempt,
			"error", err)
	}

	return fmt.Errorf("max retries (%d) exceeded for offset commit: %w", p.maxRetries, lastErr)
}

// IsPermanentError determines if an error is permanent or transient
//
// Permanent errors are configuration or permission issues that won't be fixed by retrying:
// - NoSuchBucket: Target bucket doesn't exist
// - InvalidAccessKeyId: Wrong or invalid credentials
// - AccessDenied: Valid credentials but insufficient permissions
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	// SDK v2 wraps errors in smithy OperationError, which doesn't properly
	// implement error wrapping for specific types.
	errStr := strings.ToLower(err.Error())
	permanentPatterns := []string{
		"nosuchbucket",       // Target bucket doesn't exist
		"invalidaccesskeyid", // Wrong or invalid credentials
		"accessdenied",       // Valid credentials but insufficient permissions
	}

	for _, pattern := range permanentPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// uploadLogBatch handles the upload phase: fetch, build, and upload to S3
// Returns the records and offset info needed for committing
func (p *Processor) uploadLogBatch(ctx context.Context, batch LogBatch) ([]LogRecord, time.Time, uint16, error) {
	p.logger.Info("processing log batch",
		"bucketName", batch.Bucket,
		"nLogs", batch.LogCount,
		"minTimestamp", batch.MinTimestamp,
		"maxTimestamp", batch.MaxTimestamp)

	// 1. Fetch logs
	records, err := p.logFetcher.FetchLogs(ctx, batch)
	if err != nil {
		return nil, time.Time{}, 0, fmt.Errorf("failed to fetch logs: %w", err)
	}

	if len(records) == 0 {
		p.logger.Error("no logs fetched for batch but BatchFinder expected logs",
			"bucketName", batch.Bucket,
			"expectedLogCount", batch.LogCount,
			"minTimestamp", batch.MinTimestamp,
			"maxTimestamp", batch.MaxTimestamp)
		return nil, time.Time{}, 0, fmt.Errorf("zero records fetched but BatchFinder expected %d logs for bucket %s",
			batch.LogCount, batch.Bucket)
	}

	p.logger.Debug("fetched logs", "bucketName", batch.Bucket, "nRecords", len(records))

	// 2. Build log object
	logObj, err := p.logBuilder.Build(records)
	if err != nil {
		return nil, time.Time{}, 0, fmt.Errorf("failed to build log object: %w", err)
	}

	p.logger.Debug("built log object", "bucketName", batch.Bucket, "s3Key", logObj.Key, "sizeBytes", len(logObj.Content))

	// 3. Upload to S3
	// Configuration Handling: Use logging target bucket from the first record.
	//
	// If logging configuration (loggingTargetBucket) changes mid-batch,
	// records with the new configuration will be uploaded using the old target bucket
	// until the next batch is processed.
	//
	// This is an acceptable trade-off because:
	// 1. Configuration changes are expected to be infrequent
	// 2. The propagation delay (one batch cycle) is acceptable for config changes
	destinationBucket := records[0].LoggingTargetBucket

	err = p.s3Uploader.Upload(ctx, destinationBucket, logObj.Key, logObj.Content)
	if err != nil {
		return nil, time.Time{}, 0, fmt.Errorf("failed to upload log object: %w", err)
	}

	p.logger.Info("uploaded log object",
		"bucketName", batch.Bucket,
		"destinationBucket", destinationBucket,
		"s3Key", logObj.Key,
		"sizeBytes", len(logObj.Content))

	maxInsertedAt := GetMaxInsertedAt(records)
	raftSessionID := records[0].RaftSessionID

	return records, maxInsertedAt, raftSessionID, nil
}

// GetMaxInsertedAt returns the maximum insertedAt timestamp from log records
func GetMaxInsertedAt(records []LogRecord) time.Time {
	if len(records) == 0 {
		return time.Time{}
	}

	maxTime := records[0].InsertedAt

	for i := 1; i < len(records); i++ {
		if records[i].InsertedAt.After(maxTime) {
			maxTime = records[i].InsertedAt
		}
	}

	return maxTime
}
