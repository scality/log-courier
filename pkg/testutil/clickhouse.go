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

// ClickHouseTestHelper provides utilities for testing with ClickHouse
type ClickHouseTestHelper struct {
	Client       *clickhouse.Client
	DatabaseName string
}

// NewClickHouseTestHelper creates a new test helper with a unique database
func NewClickHouseTestHelper(ctx context.Context) (*ClickHouseTestHelper, error) {
	err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Create a no-op logger for tests
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := clickhouse.Config{
		Hosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
		Username: logcourier.ConfigSpec.GetString("clickhouse.username"),
		Password: logcourier.ConfigSpec.GetString("clickhouse.password"),
		Timeout:  time.Duration(logcourier.ConfigSpec.GetInt("clickhouse.timeout-seconds")) * time.Second,
		Logger:   logger,
	}

	client, err := clickhouse.NewClient(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to test ClickHouse: %w", err)
	}

	// Generate unique database name for test isolation
	dbName := fmt.Sprintf("logs_test_%d", time.Now().UnixNano())

	return &ClickHouseTestHelper{
		Client:       client,
		DatabaseName: dbName,
	}, nil
}

// SetupSchema creates schema for testing (simplified single-node version)
func (h *ClickHouseTestHelper) SetupSchema(ctx context.Context) error {
	// Create database
	if err := h.Client.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", h.DatabaseName)); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create ingest table (Null engine - doesn't store data)
	ingestTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s
		(
			timestamp              DateTime,
			insertedAt             DateTime DEFAULT now(),
			hostname               LowCardinality(String),

			startTime              DateTime64(3),
			requester              String,
			operation              String,
			requestURI             String,
			errorCode              String,
			objectSize             UInt64,
			totalTime              Float32,
			turnAroundTime         Float32,
			referer                String,
			userAgent              String,
			versionId              String,
			signatureVersion       LowCardinality(String),
			cipherSuite            LowCardinality(String),
			authenticationType     LowCardinality(String),
			hostHeader             String,
			tlsVersion             LowCardinality(String),
			aclRequired            LowCardinality(String),

			bucketOwner            String,
			bucketName             String,
			req_id                 String,
			bytesSent              UInt64,
			clientIP               String,
			httpCode               UInt16,
			objectKey              String,

			logFormatVersion       LowCardinality(String),
			loggingEnabled         Bool,
			loggingTargetBucket    String,
			loggingTargetPrefix    String,
			awsAccessKeyID         String,
			raftSessionID          UInt16
		)
		ENGINE = Null()
	`, h.DatabaseName, clickhouse.TableAccessLogsIngest)
	if err := h.Client.Exec(ctx, ingestTableSQL); err != nil {
		return fmt.Errorf("failed to create ingest table: %w", err)
	}

	// Create access logs table (MergeTree - simplified from ReplicatedMergeTree for single-node)
	logsTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s
		AS %s.%s
		ENGINE = MergeTree()
		PARTITION BY toStartOfDay(insertedAt)
		ORDER BY (raftSessionID, bucketName, insertedAt, timestamp, req_id)
	`, h.DatabaseName, clickhouse.TableAccessLogs, h.DatabaseName, clickhouse.TableAccessLogsIngest)
	if err := h.Client.Exec(ctx, logsTableSQL); err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}

	// TODO: LOGC-21 - Implement distributed ClickHouse setup for tests.
	// For single-node tests, create federated table as a VIEW pointing to local table.
	federatedTableSQL := fmt.Sprintf(`
		CREATE VIEW IF NOT EXISTS %s.%s
		AS SELECT * FROM %s.%s
	`, h.DatabaseName, clickhouse.TableAccessLogsFederated, h.DatabaseName, clickhouse.TableAccessLogs)
	if err := h.Client.Exec(ctx, federatedTableSQL); err != nil {
		return fmt.Errorf("failed to create federated table: %w", err)
	}

	// Create materialized view that filters loggingEnabled = true
	mvSQL := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s
		TO %s.%s
		AS
		SELECT *
		FROM %s.%s
		WHERE loggingEnabled = true
	`, h.DatabaseName, clickhouse.ViewAccessLogsIngestMV, h.DatabaseName, clickhouse.TableAccessLogs, h.DatabaseName, clickhouse.TableAccessLogsIngest)
	if err := h.Client.Exec(ctx, mvSQL); err != nil {
		return fmt.Errorf("failed to create materialized view: %w", err)
	}

	// Create offsets table (MergeTree - simplified for single-node)
	offsetsTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s
		(
			bucketName                String,
			raftSessionID             UInt16,
			lastProcessedInsertedAt   DateTime,
			lastProcessedTimestamp    DateTime64(3),
			lastProcessedReqId        String
		)
		ENGINE = MergeTree()
		ORDER BY (bucketName, raftSessionID)
	`, h.DatabaseName, clickhouse.TableOffsets)
	if err := h.Client.Exec(ctx, offsetsTableSQL); err != nil {
		return fmt.Errorf("failed to create offsets table: %w", err)
	}

	return nil
}

// TeardownSchema drops all test tables and database
func (h *ClickHouseTestHelper) TeardownSchema(ctx context.Context) error {
	// Drop the entire test database (which drops all tables and views)
	if err := h.Client.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", h.DatabaseName)); err != nil {
		return fmt.Errorf("failed to drop database: %w", err)
	}

	return nil
}

// TestLogRecord represents a minimal test log record for insertion
type TestLogRecord struct {
	Timestamp      time.Time
	BucketName     string
	ReqID          string
	Action         string
	ObjectKey      string
	BytesSent      uint64
	RaftSessionID  uint16
	HttpCode       uint16
	LoggingEnabled bool
}

// InsertTestLog inserts a test log record into the ingest table
func (h *ClickHouseTestHelper) InsertTestLog(ctx context.Context, log TestLogRecord) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s
		(timestamp, insertedAt, startTime, requester, operation, requestURI, errorCode,
		 objectSize, totalTime, turnAroundTime, referer, userAgent, versionId,
		 signatureVersion, cipherSuite, authenticationType, hostHeader, tlsVersion,
		 aclRequired, bucketOwner, bucketName, req_id, bytesSent, clientIP, httpCode,
		 objectKey, loggingEnabled, loggingTargetBucket, loggingTargetPrefix, raftSessionID)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, h.DatabaseName, clickhouse.TableAccessLogsIngest)

	return h.Client.Exec(ctx, query,
		log.Timestamp,      // timestamp
		time.Now(),         // insertedAt
		log.Timestamp,      // startTime
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

// Close closes the test helper
func (h *ClickHouseTestHelper) Close() error {
	if h.Client != nil {
		return h.Client.Close()
	}
	return nil
}

// InsertTestLogWithTargetBucket inserts a test log record with logging target bucket/prefix
func (h *ClickHouseTestHelper) InsertTestLogWithTargetBucket(ctx context.Context, log TestLogRecord, targetBucket, targetPrefix string) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.%s
		(timestamp, insertedAt, startTime, requester, operation, requestURI, errorCode,
		 objectSize, totalTime, turnAroundTime, referer, userAgent, versionId,
		 signatureVersion, cipherSuite, authenticationType, hostHeader, tlsVersion,
		 aclRequired, bucketOwner, bucketName, req_id, bytesSent, clientIP, httpCode,
		 objectKey, loggingEnabled, loggingTargetBucket, loggingTargetPrefix, raftSessionID)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, h.DatabaseName, clickhouse.TableAccessLogsIngest)

	return h.Client.Exec(ctx, query,
		log.Timestamp,      // timestamp
		time.Now(),         // insertedAt
		log.Timestamp,      // startTime
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
func (f *FailingOffsetManager) CommitOffsetsBatch(ctx context.Context, requests []logcourier.OffsetCommitRequest) error {
	count := f.commitCount.Add(1)
	if count <= f.failUntilCount {
		return fmt.Errorf("simulated offset commit failure (attempt %d)", count)
	}
	return f.manager.CommitOffsetsBatch(ctx, requests)
}

// GetOffset delegates to the underlying manager
func (f *FailingOffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (logcourier.Offset, error) {
	return f.manager.GetOffset(ctx, bucket, raftSessionID)
}

// GetCommitCount returns the total number of commit attempts
func (f *FailingOffsetManager) GetCommitCount() int64 {
	return f.commitCount.Load()
}
