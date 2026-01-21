package logcourier

import (
	"context"
	"database/sql"
	"errors"
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
	CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, offset Offset) error
	CommitOffsetsBatch(ctx context.Context, requests []OffsetCommit) error
	GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (Offset, error)
}

// Offset holds the composite offset
type Offset struct {
	InsertedAt time.Time
	StartTime  int64 // Milliseconds since epoch
	ReqID      string
}

// OffsetCommit holds a single offset commit
type OffsetCommit struct {
	Offset        Offset
	Bucket        string
	RaftSessionID uint16
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

// CommitOffset commits the processing offset for a bucket using composite key
func (om *OffsetManager) CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, offset Offset) error {
	return om.CommitOffsetsBatch(ctx, []OffsetCommit{{
		Offset:        offset,
		Bucket:        bucket,
		RaftSessionID: raftSessionID,
	}})
}

// CommitOffsetsBatch commits multiple offsets in a single database operation
func (om *OffsetManager) CommitOffsetsBatch(ctx context.Context, commits []OffsetCommit) error {
	if len(commits) == 0 {
		return nil
	}

	for _, commit := range commits {
		if commit.Bucket == "" {
			return fmt.Errorf("offset commit: bucket name cannot be empty")
		}
		if commit.Offset.InsertedAt.IsZero() {
			return fmt.Errorf("offset commit: insertedAt timestamp cannot be zero")
		}
		if commit.Offset.StartTime == 0 {
			return fmt.Errorf("offset commit: startTime cannot be zero")
		}
		if commit.Offset.ReqID == "" {
			return fmt.Errorf("offset commit: reqID cannot be empty")
		}
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId)",
		om.database, clickhouse.TableOffsetsFederated)

	batch, err := om.client.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare batch for %d offsets: %w", len(commits), err)
	}

	for _, commit := range commits {
		err = batch.Append(
			commit.Bucket,
			commit.RaftSessionID,
			commit.Offset.InsertedAt,
			commit.Offset.StartTime,
			commit.Offset.ReqID,
		)
		if err != nil {
			return fmt.Errorf("failed to append offset to batch: %w", err)
		}
	}

	err = batch.Send()
	if err != nil {
		return fmt.Errorf("failed to send batch with %d offsets: %w", len(commits), err)
	}

	return nil
}

// GetOffset retrieves the current composite offset for a bucket and raft session
//
// Returns zero timestamps and empty reqID if bucket has no offset
func (om *OffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (Offset, error) {
	if bucket == "" {
		return Offset{}, fmt.Errorf("bucket name cannot be empty")
	}

	// Get the row with maximum composite offset for the given bucket and raft session.
	// Order by all three components (insertedAt, startTime, reqID) for lexicographic comparison.
	query := fmt.Sprintf(`
        SELECT
            lastProcessedInsertedAt,
            lastProcessedStartTime,
            lastProcessedReqId
        FROM %s.%s
        WHERE bucketName = ? AND raftSessionID = ?
        ORDER BY lastProcessedInsertedAt DESC, lastProcessedStartTime DESC, lastProcessedReqId DESC
        LIMIT 1
    `, om.database, clickhouse.TableOffsetsFederated)

	row := om.client.QueryRow(ctx, query, bucket, raftSessionID)

	var offset Offset
	err := row.Scan(&offset.InsertedAt, &offset.StartTime, &offset.ReqID)

	// Check if no rows returned (no offset exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Offset{}, nil
		}
		return Offset{}, fmt.Errorf("failed to get offset for bucket %s raftSessionID %d: %w", bucket, raftSessionID, err)
	}

	return offset, nil
}
