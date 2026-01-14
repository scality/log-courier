package testutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/scality/log-courier/pkg/clickhouse"
	"github.com/scality/log-courier/pkg/logcourier"
)

// ==========================================
// Helper Functions for Pointer Values
// ==========================================

// StrPtr returns a pointer to a string value
func StrPtr(s string) *string { return &s }

// Uint16Ptr returns a pointer to a uint16 value
func Uint16Ptr(n uint16) *uint16 { return &n }

// Uint64Ptr returns a pointer to a uint64 value
func Uint64Ptr(n uint64) *uint64 { return &n }

// Float32Ptr returns a pointer to a float32 value
func Float32Ptr(f float32) *float32 { return &f }

// TimePtr returns a pointer to a time.Time value
func TimePtr(t time.Time) *time.Time { return &t }

// ==========================================
// Test Schema Definitions
// ==========================================
// These schemas mirror the Federation ClickHouse setup but without replication.
// Federation schemas are maintained in roles/run-s3-analytics-clickhouse/
//
// Key differences from Federation:
//   - MergeTree instead of ReplicatedMergeTree (no replication)

const schemaAccessLogsLocal = `
CREATE TABLE IF NOT EXISTS %s.access_logs
(
	insertedAt             DateTime DEFAULT now(),
	hostname               LowCardinality(Nullable(String)),

	startTime              DateTime64(3),
	requester              Nullable(String),
	operation              Nullable(String),
	requestURI             Nullable(String),
	errorCode              Nullable(String),
	objectSize             Nullable(UInt64),
	totalTime              Nullable(Float32),
	turnAroundTime         Nullable(Float32),
	referer                Nullable(String),
	userAgent              Nullable(String),
	versionId              Nullable(String),
	signatureVersion       LowCardinality(Nullable(String)),
	cipherSuite            LowCardinality(Nullable(String)),
	authenticationType     LowCardinality(Nullable(String)),
	hostHeader             Nullable(String),
	tlsVersion             LowCardinality(Nullable(String)),
	aclRequired            LowCardinality(Nullable(String)),

	bucketOwner            Nullable(String),
	bucketName             String DEFAULT '',
	req_id                 String DEFAULT '',
	bytesSent              Nullable(UInt64),
	clientIP               Nullable(String),
	httpCode               Nullable(UInt16),
	objectKey              Nullable(String),

	logFormatVersion       LowCardinality(Nullable(String)),
	loggingEnabled         Bool DEFAULT false,
	loggingTargetBucket    String DEFAULT '',
	loggingTargetPrefix    String DEFAULT '',
	awsAccessKeyID         Nullable(String),
	raftSessionID          UInt16 DEFAULT 0,
)
ENGINE = MergeTree()
PARTITION BY toStartOfInterval(insertedAt, INTERVAL 1 HOUR)
ORDER BY (raftSessionID, bucketName, insertedAt, startTime, req_id)
`

const schemaAccessLogsFederated = `
CREATE TABLE IF NOT EXISTS %s.access_logs_federated AS %s.access_logs
ENGINE = Distributed(workbench_cluster, %s, access_logs, raftSessionID)
`

const schemaOffsetsLocal = `
CREATE TABLE IF NOT EXISTS %s.offsets
(
	bucketName                String,
	raftSessionID             UInt16,
	lastProcessedInsertedAt   DateTime,
	lastProcessedStartTime    DateTime64(3),
	lastProcessedReqId        String
)
ENGINE = ReplacingMergeTree(lastProcessedInsertedAt)
ORDER BY (bucketName, raftSessionID)
`

const schemaOffsetsFederated = `
CREATE TABLE IF NOT EXISTS %s.offsets_federated AS %s.offsets
ENGINE = Distributed(workbench_cluster, %s, offsets, raftSessionID)
`

// ==========================================
// Test Helper
// ==========================================

// ClickHouseTestHelper provides utilities for testing with ClickHouse
type ClickHouseTestHelper struct {
	Clients      []*clickhouse.Client
	DatabaseName string
	Hosts        []string
}

// Client returns the primary client (first shard) for backward compatibility
// This allows existing tests to continue using helper.Client
func (h *ClickHouseTestHelper) Client() *clickhouse.Client {
	if len(h.Clients) == 0 {
		return nil
	}
	return h.Clients[0]
}

// NewClickHouseTestHelper creates a new test helper with a unique database
func NewClickHouseTestHelper(ctx context.Context) (*ClickHouseTestHelper, error) {
	// Reset config to ensure clean state for each test (prevents config pollution between tests)
	logcourier.ConfigSpec.Reset()

	err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Create a no-op logger for tests
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	hosts := logcourier.ConfigSpec.GetStringSlice("clickhouse.url")
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no ClickHouse hosts configured")
	}

	// Generate unique database name for test isolation
	dbName := fmt.Sprintf("logs_test_%d", time.Now().UnixNano())

	// Create clients for each shard
	clients := make([]*clickhouse.Client, len(hosts))
	for i, host := range hosts {
		cfg := clickhouse.Config{
			Hosts:    []string{host}, // Single host per client
			Username: logcourier.ConfigSpec.GetString("clickhouse.username"),
			Password: logcourier.ConfigSpec.GetString("clickhouse.password"),
			Timeout:  time.Duration(logcourier.ConfigSpec.GetInt("clickhouse.timeout-seconds")) * time.Second,
			Logger:   logger,
			Settings: map[string]interface{}{
				"insert_distributed_sync": 1, // Enable synchronous inserts for tests
			},
		}

		client, err := clickhouse.NewClient(ctx, cfg)
		if err != nil {
			// Clean up any already-created clients
			for j := 0; j < i; j++ {
				_ = clients[j].Close()
			}
			return nil, fmt.Errorf("failed to connect to ClickHouse shard %d (%s): %w", i+1, host, err)
		}
		clients[i] = client
	}

	return &ClickHouseTestHelper{
		Clients:      clients,
		DatabaseName: dbName,
		Hosts:        hosts,
	}, nil
}

// SetupSchema creates distributed schema on all shards
func (h *ClickHouseTestHelper) SetupSchema(ctx context.Context) error {
	// Verify we have multiple shards configured - panic if not to catch misconfiguration immediately
	if len(h.Clients) < 2 {
		panic(fmt.Sprintf("FATAL: distributed setup requires at least 2 shards, but only %d configured (hosts: %v)", len(h.Clients), h.Hosts))
	}

	// Create database on all shards
	for i, client := range h.Clients {
		if err := client.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", h.DatabaseName)); err != nil {
			return fmt.Errorf("failed to create database on shard %d: %w", i+1, err)
		}
	}

	// Verify database exists on all shards before proceeding
	// This ensures metadata is synchronized across the cluster
	for i, client := range h.Clients {
		var dbExists uint64
		query := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s'", h.DatabaseName)
		if err := client.QueryRow(ctx, query).Scan(&dbExists); err != nil {
			return fmt.Errorf("failed to verify database on shard %d: %w", i+1, err)
		}
		if dbExists == 0 {
			return fmt.Errorf("database %s not found on shard %d after creation", h.DatabaseName, i+1)
		}
	}

	// Create local tables on all shards
	if err := h.createLocalAccessLogsTable(ctx); err != nil {
		return err
	}
	if err := h.verifyTableExists(ctx, "access_logs"); err != nil {
		return err
	}

	if err := h.createLocalOffsetsTable(ctx); err != nil {
		return err
	}
	if err := h.verifyTableExists(ctx, "offsets"); err != nil {
		return err
	}

	// Create distributed tables on all shards
	if err := h.createDistributedAccessLogsTable(ctx); err != nil {
		return err
	}
	if err := h.verifyTableExists(ctx, "access_logs_federated"); err != nil {
		return err
	}

	if err := h.createDistributedOffsetsTable(ctx); err != nil {
		return err
	}
	if err := h.verifyTableExists(ctx, "offsets_federated"); err != nil {
		return err
	}

	// Verify distributed tables are actually queryable via cluster connections
	// This ensures metadata is visible not just via direct connections but also
	// via the cluster network path that distributed queries use
	if err := h.verifyDistributedTableAccessible(ctx); err != nil {
		return err
	}

	return nil
}

func (h *ClickHouseTestHelper) createLocalAccessLogsTable(ctx context.Context) error {
	sql := fmt.Sprintf(schemaAccessLogsLocal, h.DatabaseName)
	for i, client := range h.Clients {
		if err := client.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to create local access_logs table on shard %d: %w", i+1, err)
		}
	}
	return nil
}

func (h *ClickHouseTestHelper) createLocalOffsetsTable(ctx context.Context) error {
	sql := fmt.Sprintf(schemaOffsetsLocal, h.DatabaseName)
	for i, client := range h.Clients {
		if err := client.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to create local offsets table on shard %d: %w", i+1, err)
		}
	}
	return nil
}

func (h *ClickHouseTestHelper) createDistributedAccessLogsTable(ctx context.Context) error {
	sql := fmt.Sprintf(schemaAccessLogsFederated, h.DatabaseName, h.DatabaseName, h.DatabaseName)

	for i, client := range h.Clients {
		if err := client.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to create distributed access_logs_federated table on shard %d: %w", i+1, err)
		}
	}
	return nil
}

func (h *ClickHouseTestHelper) createDistributedOffsetsTable(ctx context.Context) error {
	sql := fmt.Sprintf(schemaOffsetsFederated, h.DatabaseName, h.DatabaseName, h.DatabaseName)

	for i, client := range h.Clients {
		if err := client.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to create distributed offsets_federated table on shard %d: %w", i+1, err)
		}
	}
	return nil
}

// verifyTableExists ensures a table exists on all shards
// This ensures metadata is synchronized across the cluster
func (h *ClickHouseTestHelper) verifyTableExists(ctx context.Context, tableName string) error {
	for i, client := range h.Clients {
		var tableExists uint64
		query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'", h.DatabaseName, tableName)
		if err := client.QueryRow(ctx, query).Scan(&tableExists); err != nil {
			return fmt.Errorf("failed to verify table %s on shard %d: %w", tableName, i+1, err)
		}
		if tableExists == 0 {
			return fmt.Errorf("table %s.%s not found on shard %d after creation", h.DatabaseName, tableName, i+1)
		}
	}
	return nil
}

// verifyDistributedTableAccessible ensures distributed tables can actually query
// the local tables on all shards via cluster connections. This catches metadata
// visibility issues that might not be detected by direct shard queries.
func (h *ClickHouseTestHelper) verifyDistributedTableAccessible(ctx context.Context) error {
	// Simple verification: try to query the distributed table from each shard
	for i, client := range h.Clients {
		query := fmt.Sprintf("SELECT count() FROM %s.access_logs_federated", h.DatabaseName)
		var count uint64
		if err := client.QueryRow(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("shard %d cannot query access_logs_federated: %w", i+1, err)
		}

		query = fmt.Sprintf("SELECT count() FROM %s.offsets_federated", h.DatabaseName)
		if err := client.QueryRow(ctx, query).Scan(&count); err != nil {
			return fmt.Errorf("shard %d cannot query offsets_federated: %w", i+1, err)
		}
	}

	return nil
}

// TeardownSchema drops all test tables and database from all shards
func (h *ClickHouseTestHelper) TeardownSchema(ctx context.Context) error {
	for i, client := range h.Clients {
		if err := client.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", h.DatabaseName)); err != nil {
			return fmt.Errorf("failed to drop database on shard %d: %w", i+1, err)
		}
	}
	return nil
}

// TestLogRecord represents a minimal test log record for insertion
type TestLogRecord struct {
	StartTime      time.Time
	BucketName     string
	ReqID          string
	Action         string
	ObjectKey      string
	BytesSent      uint64
	RaftSessionID  uint16
	HttpCode       uint16
	LoggingEnabled bool
}

// InsertTestLog inserts a test log record into the federated table
// The Distributed engine will route to the correct shard based on raftSessionID
func (h *ClickHouseTestHelper) InsertTestLog(ctx context.Context, log TestLogRecord) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s
		(insertedAt, startTime, requester, operation, requestURI, errorCode,
		 objectSize, totalTime, turnAroundTime, referer, userAgent, versionId,
		 signatureVersion, cipherSuite, authenticationType, hostHeader, tlsVersion,
		 aclRequired, bucketOwner, bucketName, req_id, bytesSent, clientIP, httpCode,
		 objectKey, loggingEnabled, loggingTargetBucket, loggingTargetPrefix, raftSessionID)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, h.DatabaseName, clickhouse.TableAccessLogsFederated)

	return h.Clients[0].Exec(ctx, query,
		time.Now(),         // insertedAt
		log.StartTime,      // startTime
		"",                 // requester
		log.Action,         // operation
		"",                 // requestURI
		"",                 // errorCode
		uint64(0),          // objectSize
		float32(0),         // totalTime
		float32(0),         // turnAroundTime
		"",                 // referer
		"",                 // userAgent
		"",                 // versionId
		"",                 // signatureVersion
		"",                 // cipherSuite
		"",                 // authenticationType
		"",                 // hostHeader
		"",                 // tlsVersion
		"",                 // aclRequired
		"",                 // bucketOwner
		log.BucketName,     // bucketName
		log.ReqID,          // req_id
		log.BytesSent,      // bytesSent
		"",                 // clientIP
		log.HttpCode,       // httpCode
		log.ObjectKey,      // objectKey
		log.LoggingEnabled, // loggingEnabled
		"",                 // loggingTargetBucket
		"",                 // loggingTargetPrefix
		log.RaftSessionID,  // raftSessionID
	)
}

// Close closes all shard clients
func (h *ClickHouseTestHelper) Close() error {
	var errs []error
	for i, client := range h.Clients {
		if client != nil {
			if err := client.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close client %d: %w", i+1, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing clients: %v", errs)
	}
	return nil
}

// InsertTestLogWithTargetBucket inserts a test log record with logging target bucket/prefix
// The Distributed engine will route to the correct shard based on raftSessionID
func (h *ClickHouseTestHelper) InsertTestLogWithTargetBucket(ctx context.Context, log TestLogRecord, targetBucket, targetPrefix string) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s
		(insertedAt, startTime, requester, operation, requestURI, errorCode,
		 objectSize, totalTime, turnAroundTime, referer, userAgent, versionId,
		 signatureVersion, cipherSuite, authenticationType, hostHeader, tlsVersion,
		 aclRequired, bucketOwner, bucketName, req_id, bytesSent, clientIP, httpCode,
		 objectKey, loggingEnabled, loggingTargetBucket, loggingTargetPrefix, raftSessionID)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, h.DatabaseName, clickhouse.TableAccessLogsFederated)

	return h.Clients[0].Exec(ctx, query,
		time.Now(),         // insertedAt
		log.StartTime,      // startTime
		"",                 // requester
		log.Action,         // operation
		"",                 // requestURI
		"",                 // errorCode
		uint64(0),          // objectSize
		float32(0),         // totalTime
		float32(0),         // turnAroundTime
		"",                 // referer
		"",                 // userAgent
		"",                 // versionId
		"",                 // signatureVersion
		"",                 // cipherSuite
		"",                 // authenticationType
		"",                 // hostHeader
		"",                 // tlsVersion
		"",                 // aclRequired
		"",                 // bucketOwner
		log.BucketName,     // bucketName
		log.ReqID,          // req_id
		log.BytesSent,      // bytesSent
		"",                 // clientIP
		log.HttpCode,       // httpCode
		log.ObjectKey,      // objectKey
		log.LoggingEnabled, // loggingEnabled
		targetBucket,       // loggingTargetBucket
		targetPrefix,       // loggingTargetPrefix
		log.RaftSessionID,  // raftSessionID
	)
}

// FailingOffsetManager wraps an OffsetManager and injects failures for testing
type FailingOffsetManager struct {
	manager        logcourier.OffsetManagerInterface
	commitCount    atomic.Int64
	failUntilCount int64
}

// NewFailingOffsetManager creates a new failing offset manager wrapper
func NewFailingOffsetManager(manager logcourier.OffsetManagerInterface, failUntilCount int64) *FailingOffsetManager {
	return &FailingOffsetManager{
		manager:        manager,
		failUntilCount: failUntilCount,
	}
}

// CommitOffset wraps the underlying manager's CommitOffset and fails until failUntilCount
func (f *FailingOffsetManager) CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, offset logcourier.Offset) error {
	count := f.commitCount.Add(1)
	if count <= f.failUntilCount {
		return fmt.Errorf("simulated offset commit failure (attempt %d)", count)
	}
	return f.manager.CommitOffset(ctx, bucket, raftSessionID, offset)
}

// CommitOffsetsBatch wraps the underlying manager's CommitOffsetsBatch and fails until failUntilCount
func (f *FailingOffsetManager) CommitOffsetsBatch(ctx context.Context, commits []logcourier.OffsetCommit) error {
	count := f.commitCount.Add(1)
	if count <= f.failUntilCount {
		return fmt.Errorf("simulated offset commit failure (attempt %d)", count)
	}
	return f.manager.CommitOffsetsBatch(ctx, commits)
}

// GetOffset delegates to the underlying manager
func (f *FailingOffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (logcourier.Offset, error) {
	return f.manager.GetOffset(ctx, bucket, raftSessionID)
}

// GetCommitCount returns the total number of commit attempts
func (f *FailingOffsetManager) GetCommitCount() int64 {
	return f.commitCount.Load()
}
