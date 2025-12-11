package logcourier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/scality/log-courier/pkg/clickhouse"
	"github.com/scality/log-courier/pkg/s3"
)

const (
	// backoffMultiplier is the exponential backoff multiplier for retry attempts
	backoffMultiplier = 2.0
)

// applyJitter applies symmetric jitter to a duration.
//
// The jitter creates a random duration centered on the input duration, varying by jitterFactor (+ or -)
// For example, with jitterFactor=0.2 and duration=60s, the result ranges from 48s to 72s (+/-20%).
//
// Returns the original duration if jitterFactor is 0 or negative.
func applyJitter(duration time.Duration, jitterFactor float64) time.Duration {
	if jitterFactor <= 0 {
		return duration
	}

	// Generate random multiplier in [1-jitterFactor, 1+jitterFactor]
	// rand.Float64()*2.0 - 1.0 produces [-1, 1]
	// Multiply by jitterFactor to get [-jitterFactor, +jitterFactor]
	// Add 1.0 to center around the base duration
	//nolint:gosec // Using non-cryptographic random for jitter is acceptable
	multiplier := 1.0 + (rand.Float64()*2.0-1.0)*jitterFactor
	return time.Duration(float64(duration) * multiplier)
}

// Processor is the main log courier processor
type Processor struct {
	// Components
	clickhouseClient *clickhouse.Client
	s3Uploader       s3.UploaderInterface
	workDiscovery    *BatchFinder
	logFetcher       *LogFetcher
	logBuilder       *LogObjectBuilder
	offsetManager    OffsetManagerInterface
	offsetBuffer     *OffsetBuffer
	logger           *slog.Logger

	// Configuration
	minDiscoveryInterval          time.Duration
	maxDiscoveryInterval          time.Duration
	discoveryIntervalJitterFactor float64
	numWorkers                    int
	maxRetries                    int
	initialBackoff                time.Duration
	maxBackoff                    time.Duration
	backoffJitterFactor           float64
	uploadOperationTimeout        time.Duration
	commitOperationTimeout        time.Duration
}

// ProcessResult holds the result of uploading a log batch
type ProcessResult struct {
	Offset        Offset
	Records       []LogRecord
	RaftSessionID uint16
}

// Config holds processor configuration
//
//nolint:govet // Field alignment is less important than readability for config structs
type Config struct {
	Logger *slog.Logger

	ClickHouseTimeout time.Duration

	// MinDiscoveryInterval is the minimum interval between discovery runs (when work is found)
	MinDiscoveryInterval time.Duration
	// MaxDiscoveryInterval is the maximum interval between discovery runs (when no work is found)
	MaxDiscoveryInterval time.Duration
	// DiscoveryIntervalJitterFactor is the jitter factor for discovery interval (0.0 to 1.0)
	DiscoveryIntervalJitterFactor float64

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
	// BackoffJitterFactor is the jitter factor for backoff (0.0 to 1.0)
	BackoffJitterFactor float64

	// UploadOperationTimeout is the maximum time for upload operations (fetch + build + upload)
	UploadOperationTimeout time.Duration
	// CommitOperationTimeout is the maximum time for offset commit operations
	CommitOperationTimeout time.Duration

	ClickHouseHosts    []string
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
		Hosts:          cfg.ClickHouseHosts,
		Username:       cfg.ClickHouseUsername,
		Password:       cfg.ClickHousePassword,
		Timeout:        cfg.ClickHouseTimeout,
		MaxRetries:     cfg.MaxRetries,
		InitialBackoff: cfg.InitialBackoff,
		MaxBackoff:     cfg.MaxBackoff,
		Logger:         cfg.Logger,
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

	// Apply default timeouts if not configured (for tests that don't use config system)
	uploadTimeout := cfg.UploadOperationTimeout
	if uploadTimeout == 0 {
		uploadTimeout = 5 * time.Minute // Default: 300 seconds
	}
	commitTimeout := cfg.CommitOperationTimeout
	if commitTimeout == 0 {
		commitTimeout = 30 * time.Second // Default: 30 seconds
	}

	// Initialize offset buffer with config
	offsetBuffer := NewOffsetBuffer(OffsetBufferConfig{
		MaxRetries:          cfg.MaxRetries,
		InitialBackoff:      cfg.InitialBackoff,
		MaxBackoff:          cfg.MaxBackoff,
		BackoffJitterFactor: cfg.BackoffJitterFactor,
		OffsetManager:       offsetManager,
		Logger:              cfg.Logger,
	})

	return &Processor{
		clickhouseClient:              chClient,
		s3Uploader:                    s3Uploader,
		workDiscovery:                 NewBatchFinder(chClient, database, cfg.CountThreshold, cfg.TimeThresholdSec),
		logFetcher:                    NewLogFetcher(chClient, database),
		logBuilder:                    NewLogObjectBuilder(),
		offsetManager:                 offsetManager,
		offsetBuffer:                  offsetBuffer,
		minDiscoveryInterval:          cfg.MinDiscoveryInterval,
		maxDiscoveryInterval:          cfg.MaxDiscoveryInterval,
		discoveryIntervalJitterFactor: cfg.DiscoveryIntervalJitterFactor,
		numWorkers:                    cfg.NumWorkers,
		maxRetries:                    cfg.MaxRetries,
		initialBackoff:                cfg.InitialBackoff,
		maxBackoff:                    cfg.MaxBackoff,
		backoffJitterFactor:           cfg.BackoffJitterFactor,
		uploadOperationTimeout:        uploadTimeout,
		commitOperationTimeout:        commitTimeout,
		logger:                        cfg.Logger,
	}, nil
}

// Close closes the processor and releases resources
func (p *Processor) Close() error {
	// Flush any remaining buffered offsets before closing
	if p.offsetBuffer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := p.offsetBuffer.Flush(ctx); err != nil {
			p.logger.Error("failed to flush offsets during shutdown", "error", err)
			// Continue with cleanup even if flush fails
		}
	}

	if p.clickhouseClient != nil {
		return p.clickhouseClient.Close()
	}
	return nil
}

// Run runs the processor main loop
//
// The processor runs cycles indefinitely. If errors occur during a cycle,
// they are logged and the processor waits for the next discovery interval
// before retrying (eventual delivery).
func (p *Processor) Run(ctx context.Context) error {
	p.logger.Info("processor starting")

	// Run initial cycle
	cycleStart := time.Now()
	batchCount, err := p.runCycle(ctx)
	if err != nil {
		p.logger.Error("cycle failed", "error", err)
		batchCount = 0 // Treat error as no work found
	}

	// Mandatory flush after cycle completes
	// This ensures the next cycle's BatchFinder sees up-to-date offsets.
	// Without this flush, BatchFinder would query stale offsets from ClickHouse
	// and rediscover batches we've already processed.
	//
	// Batching benefit: If we processed N batches this cycle, we do 1 ClickHouse
	// write instead of N writes.
	if flushErr := p.offsetBuffer.Flush(ctx); flushErr != nil {
		p.logger.Error("failed to flush offsets at cycle boundary", "error", flushErr)
		// Continue anyway - offsets remain buffered for next cycle
	}

	for {
		// Select base interval based on work found
		baseInterval := p.maxDiscoveryInterval
		intervalMode := "max"
		if batchCount > 0 {
			baseInterval = p.minDiscoveryInterval
			intervalMode = "min"
		}

		// Apply jitter to base interval
		jitteredInterval := applyJitter(baseInterval, p.discoveryIntervalJitterFactor)

		// Calculate sleep duration accounting for processing time
		processingTime := time.Since(cycleStart)
		sleepDuration := jitteredInterval - processingTime
		if sleepDuration < 0 {
			sleepDuration = 0
		}

		p.logger.Debug("scheduling next discovery cycle",
			"batchesFound", batchCount,
			"processingTimeSeconds", processingTime.Seconds(),
			"baseIntervalSeconds", baseInterval.Seconds(),
			"jitteredIntervalSeconds", jitteredInterval.Seconds(),
			"sleepDurationSeconds", sleepDuration.Seconds(),
			"intervalMode", intervalMode)

		select {
		case <-ctx.Done():
			p.logger.Info("processor stopping")
			return ctx.Err()

		case <-time.After(sleepDuration):
			cycleStart = time.Now()
			batchCount, err = p.runCycle(ctx)
			if err != nil {
				p.logger.Error("cycle failed", "error", err)
				batchCount = 0 // Treat error as no work found
			}

			// Mandatory flush after cycle completes (see comment above)
			if flushErr := p.offsetBuffer.Flush(ctx); flushErr != nil {
				p.logger.Error("failed to flush offsets at cycle boundary", "error", flushErr)
				// Continue anyway - offsets remain buffered for next cycle
			}
		}
	}
}

// runCycle executes a single discovery and processing cycle
// Returns the number of batches found
func (p *Processor) runCycle(ctx context.Context) (int, error) {
	batchCount, err := p.runBatchFinder(ctx)
	if err != nil {
		return 0, fmt.Errorf("batch finder failed: %w", err)
	}

	return batchCount, nil
}

// runBatchFinder executes batch finder and processes log batches in parallel
// Returns the number of batches found
func (p *Processor) runBatchFinder(ctx context.Context) (int, error) {
	// Phase 1: Work Discovery
	p.logger.Debug("starting batch finder")

	batches, err := p.workDiscovery.FindBatches(ctx)
	if err != nil {
		return 0, fmt.Errorf("batch finder failed: %w", err)
	}

	p.logger.Info("batch finder completed", "nBatches", len(batches))

	if len(batches) == 0 {
		return 0, nil
	}

	// Phase 2: Process log batches in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successCount int
	var failedBatches []string

	sem := make(chan struct{}, p.numWorkers)

	for _, batch := range batches {
		batch := batch
		wg.Add(1)

		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			if err := p.processLogBatchWithRetry(ctx, batch); err != nil {
				mu.Lock()
				failedBatches = append(failedBatches, batch.Bucket)
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
		}()
	}

	wg.Wait()

	if len(failedBatches) > 0 {
		p.logger.Warn("batch processing completed",
			"totalBatches", len(batches),
			"successful", successCount,
			"failed", len(failedBatches),
			"failedBuckets", failedBatches)
	} else {
		p.logger.Info("batch processing completed",
			"totalBatches", len(batches),
			"successful", successCount,
			"failed", 0)
	}

	return len(batches), nil
}

// ============================================================================
// BATCH PROCESSING ERROR HANDLING
// ============================================================================
//
// Two-phase processing:
//   1. Upload: Fetch logs → build object → upload to S3
//   2. Offset commit: Record max timestamp in ClickHouse
//
// Error handling:
//   - Permanent errors (NoSuchBucket, InvalidAccessKeyId, AccessDenied): No retry
//   - Transient errors: Retry with exponential backoff (up to maxRetries)
//   - Offset commit: All errors are transient and retried
//
// At-least-once delivery:
//   If offset commit fails after successful upload, logs are reprocessed
//   on the next cycle, creating duplicate S3 objects.
//
// Processor continues indefinitely regardless of batch success or failure,
// retrying on next discovery interval.
// ============================================================================

func (p *Processor) processLogBatchWithRetry(ctx context.Context, batch LogBatch) error {
	result, err := p.uploadLogBatchWithRetry(ctx, batch)
	if err != nil {
		return err
	}

	// Buffer offset (will be flushed at cycle boundary)
	p.offsetBuffer.Put(batch.Bucket, result.RaftSessionID, result.Offset)

	return nil
}

// retryWithBackoff executes an operation with exponential backoff retry logic
func (p *Processor) retryWithBackoff(
	ctx context.Context,
	operation func() error,
	shouldRetry func(error) bool,
	operationName string,
	logger *slog.Logger,
) error {
	var lastErr error
	backoff := p.initialBackoff

	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		if attempt > 0 {
			// Apply jitter to the backoff
			actualBackoff := applyJitter(backoff, p.backoffJitterFactor)

			logger.Info(fmt.Sprintf("retrying %s after backoff", operationName),
				"attempt", attempt,
				"backoffSeconds", actualBackoff.Seconds())

			select {
			case <-time.After(actualBackoff):
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					logger.Warn(fmt.Sprintf("%s operation timed out", operationName), "error", ctx.Err())
				}
				return ctx.Err()
			}

			backoff = time.Duration(float64(backoff) * backoffMultiplier)
			if backoff > p.maxBackoff {
				backoff = p.maxBackoff
			}

			logger.Debug(fmt.Sprintf("executing %s retry", operationName), "attempt", attempt)
		}

		err := operation()
		if err == nil {
			logger.Info(fmt.Sprintf("%s succeeded", operationName), "attempt", attempt)
			return nil
		}

		if shouldRetry != nil && !shouldRetry(err) {
			logger.Error(fmt.Sprintf("permanent error, not retrying %s", operationName), "error", err)
			return fmt.Errorf("permanent error in %s: %w", operationName, err)
		}

		lastErr = err

		logger.Warn(fmt.Sprintf("transient error, will retry %s", operationName),
			"attempt", attempt,
			"error", err)
	}

	return fmt.Errorf("max retries (%d) exceeded for %s: %w", p.maxRetries, operationName, lastErr)
}

// uploadLogBatchWithRetry handles fetching, building, and uploading with retries
// Returns the process result needed for committing
func (p *Processor) uploadLogBatchWithRetry(ctx context.Context, batch LogBatch) (*ProcessResult, error) {
	var result *ProcessResult

	logger := p.logger.With("bucketName", batch.Bucket)

	uploadCtx, cancel := context.WithTimeout(ctx, p.uploadOperationTimeout)
	defer cancel()

	err := p.retryWithBackoff(uploadCtx, func() error {
		var err error
		result, err = p.uploadLogBatch(uploadCtx, batch)
		return err
	}, func(err error) bool {
		return !IsPermanentError(err)
	}, "upload", logger)

	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		p.logger.Error("upload operation exceeded timeout",
			"bucketName", batch.Bucket,
			"timeout", p.uploadOperationTimeout,
			"error", err)
	}

	return result, err
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
func (p *Processor) uploadLogBatch(ctx context.Context, batch LogBatch) (*ProcessResult, error) {
	p.logger.Info("processing log batch",
		"bucketName", batch.Bucket,
		"nLogs", batch.LogCount,
		"minTimestamp", batch.MinTimestamp,
		"maxTimestamp", batch.MaxTimestamp)

	// 1. Fetch logs
	records, err := p.logFetcher.FetchLogs(ctx, batch)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs: %w", err)
	}

	if len(records) == 0 {
		p.logger.Error("no logs fetched for batch but BatchFinder expected logs",
			"bucketName", batch.Bucket,
			"expectedLogCount", batch.LogCount,
			"minTimestamp", batch.MinTimestamp,
			"maxTimestamp", batch.MaxTimestamp)
		return nil, fmt.Errorf("zero records fetched but BatchFinder expected %d logs for bucket %s",
			batch.LogCount, batch.Bucket)
	}

	p.logger.Debug("fetched logs", "bucketName", batch.Bucket, "nRecords", len(records))

	// 2. Build log object
	logObj, err := p.logBuilder.Build(records)
	if err != nil {
		return nil, fmt.Errorf("failed to build log object: %w", err)
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
		return nil, fmt.Errorf("failed to upload log object: %w", err)
	}

	p.logger.Info("uploaded log object",
		"bucketName", batch.Bucket,
		"destinationBucket", destinationBucket,
		"s3Key", logObj.Key,
		"sizeBytes", len(logObj.Content))

	// Get triple composite offset from last log
	lastLog := records[len(records)-1]

	return &ProcessResult{
		Records: records,
		Offset: Offset{
			InsertedAt: lastLog.InsertedAt,
			Timestamp:  lastLog.Timestamp,
			ReqID:      lastLog.ReqID,
		},
		RaftSessionID: lastLog.RaftSessionID,
	}, nil
}
