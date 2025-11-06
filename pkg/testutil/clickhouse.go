package testutil

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"

	"github.com/scality/log-courier/pkg/clickhouse"
	"github.com/scality/log-courier/pkg/logcourier"
)

const (
	// DefaultMaterializedViewPollInterval is how often to check if materialized view has processed data
	DefaultMaterializedViewPollInterval = 10 * time.Millisecond

	// DefaultMaterializedViewTimeout is how long to wait for materialized view before timing out
	DefaultMaterializedViewTimeout = 5 * time.Second
)

// ClickHouseTestHelper provides utilities for testing with ClickHouse
type ClickHouseTestHelper struct {
	Client       *clickhouse.Client
	DatabaseName string
}

// NewClickHouseTestHelper creates a new test helper with a unique database
func NewClickHouseTestHelper(ctx context.Context) (*ClickHouseTestHelper, error) {
	for key, spec := range logcourier.ConfigSpec {
		viper.SetDefault(key, spec.DefaultValue)
		if spec.EnvVar != "" {
			_ = viper.BindEnv(key, spec.EnvVar)
		}
	}

	cfg := clickhouse.Config{
		URL:      logcourier.ConfigSpec.GetString("clickhouse.url"),
		Username: logcourier.ConfigSpec.GetString("clickhouse.username"),
		Password: logcourier.ConfigSpec.GetString("clickhouse.password"),
		Timeout:  time.Duration(logcourier.ConfigSpec.GetInt("clickhouse.timeout-seconds")) * time.Second,
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
		CREATE TABLE IF NOT EXISTS %s.access_logs_ingest
		(
			timestamp              DateTime,
			startTime              DateTime,
			hostname               LowCardinality(String),
			action                 LowCardinality(String),
			accountName            String,
			accountDisplayName     String,
			bucketName             String,
			bucketOwner            String,
			userName               String,
			requester              String,

			httpMethod             LowCardinality(String),
			httpCode               UInt16,
			httpURL                String,
			errorCode              String,
			objectKey              String,

			versionId              String,

			bytesDeleted           UInt64,
			bytesReceived          UInt64,
			bytesSent              UInt64,
			bodyLength             UInt64,
			contentLength          UInt64,

			clientIP               String,
			referer                String,
			userAgent              String,
			hostHeader             String,

			elapsed_ms             Float32,
			turnAroundTime         Float32,

			req_id                 String,
			raftSessionId          UInt16,

			signatureVersion       LowCardinality(String),
			cipherSuite            LowCardinality(String),
			authenticationType     LowCardinality(String),
			tlsVersion             LowCardinality(String),
			aclRequired            LowCardinality(String),

			logFormatVersion       LowCardinality(String),
			loggingEnabled         Bool,
			loggingTargetBucket    String,
			loggingTargetPrefix    String,

			insertedAt             DateTime DEFAULT now()
		)
		ENGINE = Null()
	`, h.DatabaseName)
	if err := h.Client.Exec(ctx, ingestTableSQL); err != nil {
		return fmt.Errorf("failed to create ingest table: %w", err)
	}

	// Create access logs table (MergeTree - simplified from ReplicatedMergeTree for single-node)
	logsTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.access_logs
		AS %s.access_logs_ingest
		ENGINE = MergeTree()
		PARTITION BY toStartOfDay(insertedAt)
		ORDER BY (raftSessionId, bucketName, insertedAt, req_id)
	`, h.DatabaseName, h.DatabaseName)
	if err := h.Client.Exec(ctx, logsTableSQL); err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}

	// Create materialized view that filters loggingEnabled = true
	mvSQL := fmt.Sprintf(`
		CREATE MATERIALIZED VIEW IF NOT EXISTS %s.access_logs_ingest_mv
		TO %s.access_logs
		AS
		SELECT *
		FROM %s.access_logs_ingest
		WHERE loggingEnabled = true
	`, h.DatabaseName, h.DatabaseName, h.DatabaseName)
	if err := h.Client.Exec(ctx, mvSQL); err != nil {
		return fmt.Errorf("failed to create materialized view: %w", err)
	}

	// Create offsets table (MergeTree - simplified for single-node)
	offsetsTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.offsets
		(
			bucketName            String,
			raftSessionId         UInt16,
			last_processed_ts     DateTime
		)
		ENGINE = MergeTree()
		ORDER BY (bucketName, raftSessionId)
	`, h.DatabaseName)
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
		INSERT INTO %s.access_logs_ingest
		(timestamp, bucketName, req_id, action, loggingEnabled, raftSessionId,
		 httpCode, bytesSent, startTime, hostname, accountName, accountDisplayName,
		 bucketOwner, userName, requester, httpMethod, httpURL, errorCode, objectKey, versionId,
		 bytesDeleted, bytesReceived, bodyLength, contentLength, clientIP, referer,
		 userAgent, hostHeader, elapsed_ms, turnAroundTime, signatureVersion,
		 cipherSuite, authenticationType, tlsVersion, aclRequired, logFormatVersion,
		 loggingTargetBucket, loggingTargetPrefix)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, h.DatabaseName)

	return h.Client.Exec(ctx, query,
		log.Timestamp,
		log.BucketName,
		log.ReqID,
		log.Action,
		log.LoggingEnabled,
		log.RaftSessionID,
		log.HttpCode,
		log.BytesSent,
		// Provide defaults for other required fields
		log.Timestamp,     // startTime
		"",                // hostname
		"",                // accountName
		"",                // accountDisplayName
		"",                // bucketOwner
		"",                // userName
		"",                // requester
		"",                // httpMethod
		"",                // httpURL
		"",                // errorCode
		log.ObjectKey,     // objectKey
		"",                // versionId
		uint64(0),         // bytesDeleted
		uint64(0),         // bytesReceived
		uint64(0),         // bodyLength
		uint64(0),         // contentLength
		"",                // clientIP
		"",                // referer
		"",                // userAgent
		"",                // hostHeader
		float32(0),        // elapsed_ms
		float32(0),        // turnAroundTime
		"",                // signatureVersion
		"",                // cipherSuite
		"",                // authenticationType
		"",                // tlsVersion
		"",                // aclRequired
		"",                // logFormatVersion
		"",                // loggingTargetBucket
		"",                // loggingTargetPrefix
	)
}

// WaitForMaterializedView polls until the expected record appears in the target table
func (h *ClickHouseTestHelper) WaitForMaterializedView(
	ctx context.Context,
	conditionQuery string,
	timeout time.Duration,
) error {
	if timeout == 0 {
		timeout = DefaultMaterializedViewTimeout
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(DefaultMaterializedViewPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			var count uint64
			err := h.Client.QueryRow(ctx, conditionQuery).Scan(&count)
			if err != nil {
				return fmt.Errorf("failed to query condition: %w", err)
			}

			if count > 0 {
				return nil // Condition met
			}

			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for materialized view after %v", timeout)
			}
		}
	}
}

// Close closes the test helper
func (h *ClickHouseTestHelper) Close() error {
	if h.Client != nil {
		return h.Client.Close()
	}
	return nil
}
