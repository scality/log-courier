package logcourier

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/scality/log-courier/pkg/clickhouse"
)

// Consistency Guarantees
//
// 1. At-least-once delivery:
//    - Offsets are committed AFTER successful S3 upload
//    - If upload succeeds but offset commit fails, logs may be reprocessed and duplicated
//    - If consumer crashes after S3 upload but before offset commit, logs will be reprocessed
//
// 2. Offset semantics:
//    - Offset is max(inserted_at) from processed log records
//    - On restart, consumer processes logs with inserted_at > last committed offset
//
// 3. ReplacingMergeTree behavior:
//    - Multiple offset commits for same bucket: latest timestamp wins
//    - Eventually consistent (ClickHouse merges may be delayed)
//    - Work discovery query uses max(last_processed_ts) to get latest offset

// OffsetManagerInterface defines the interface for offset management
type OffsetManagerInterface interface {
	CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, insertedAt time.Time, timestamp time.Time, reqID string) error
	GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (OffsetTimestamps, error)
}

// OffsetTimestamps holds the triple composite offset
type OffsetTimestamps struct {
	InsertedAt time.Time
	Timestamp  time.Time
	ReqID      string
}

// OffsetManager manages offsets
type OffsetManager struct {
	client   *clickhouse.Client
	database string
}

// NewOffsetManager creates a new offset manager
func NewOffsetManager(client *clickhouse.Client, database string) *OffsetManager {
	return &OffsetManager{
		client:   client,
		database: database,
	}
}

// CommitOffset commits the processing offset for a bucket using triple composite key
func (om *OffsetManager) CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, insertedAt time.Time, timestamp time.Time, reqID string) error {
	if bucket == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}
	if insertedAt.IsZero() {
		return fmt.Errorf("insertedAt timestamp cannot be zero")
	}
	if timestamp.IsZero() {
		return fmt.Errorf("timestamp cannot be zero")
	}
	if reqID == "" {
		return fmt.Errorf("reqID cannot be empty")
	}

	query := fmt.Sprintf(`
        INSERT INTO %s.%s (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedTimestamp, lastProcessedReqId)
        VALUES (?, ?, ?, ?, ?)
    `, om.database, clickhouse.TableOffsets)

	err := om.client.Exec(ctx, query, bucket, raftSessionID, insertedAt, timestamp, reqID)
	if err != nil {
		return fmt.Errorf("failed to commit offset for bucket %s raftSessionID %d: %w", bucket, raftSessionID, err)
	}

	return nil
}

// GetOffset retrieves the current triple composite offset for a bucket and raft session
//
// Returns zero timestamps and empty reqID if bucket has no offset
func (om *OffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (OffsetTimestamps, error) {
	if bucket == "" {
		return OffsetTimestamps{}, fmt.Errorf("bucket name cannot be empty")
	}

	// Use JOIN pattern to get full row with maximum insertedAt (preserves DateTime64 precision)
	query := fmt.Sprintf(`
        SELECT
            o.lastProcessedInsertedAt,
            o.lastProcessedTimestamp,
            o.lastProcessedReqId
        FROM %s.%s o
        INNER JOIN (
            SELECT max(lastProcessedInsertedAt) as maxInsertedAt
            FROM %s.%s
            WHERE bucketName = ? AND raftSessionID = ?
        ) m ON o.lastProcessedInsertedAt = m.maxInsertedAt
        WHERE o.bucketName = ? AND o.raftSessionID = ?
        LIMIT 1
    `, om.database, clickhouse.TableOffsets, om.database, clickhouse.TableOffsets)

	row := om.client.QueryRow(ctx, query, bucket, raftSessionID, bucket, raftSessionID)

	var insertedAt sql.NullTime
	var timestamp sql.NullTime
	var reqID sql.NullString
	err := row.Scan(&insertedAt, &timestamp, &reqID)

	// Check if no rows returned (no offset exists)
	if err != nil {
		if err.Error() == "sql: no rows in result set" || err.Error() == "EOF" {
			return OffsetTimestamps{}, nil
		}
		return OffsetTimestamps{}, fmt.Errorf("failed to get offset for bucket %s raftSessionID %d: %w", bucket, raftSessionID, err)
	}

	// If no offset exists, return zero values
	if !insertedAt.Valid {
		return OffsetTimestamps{}, nil
	}

	// Build result - handle NULL values from old offset format
	result := OffsetTimestamps{
		InsertedAt: insertedAt.Time,
	}
	if timestamp.Valid {
		result.Timestamp = timestamp.Time
	}
	if reqID.Valid {
		result.ReqID = reqID.String
	}

	return result, nil
}
