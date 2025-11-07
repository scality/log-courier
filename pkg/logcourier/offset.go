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

// CommitOffset commits the processing offset for a bucket
func (om *OffsetManager) CommitOffset(ctx context.Context, bucket string, raftSessionId uint16, timestamp time.Time) error {
	if bucket == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}
	if timestamp.IsZero() {
		return fmt.Errorf("timestamp cannot be zero")
	}

	query := fmt.Sprintf(`
        INSERT INTO %s.%s (bucketName, raftSessionId, last_processed_ts)
        VALUES (?, ?, ?)
    `, om.database, clickhouse.TableOffsets)

	err := om.client.Exec(ctx, query, bucket, raftSessionId, timestamp)
	if err != nil {
		return fmt.Errorf("failed to commit offset for bucket %s raftSessionId %d: %w", bucket, raftSessionId, err)
	}

	return nil
}

// GetOffset retrieves the current offset for a bucket and raft session
//
// Both bucket and raftSessionId are used so that there are two separate offsets for migrated buckets,
// one for the source and one for the destination raft session.
//
// Returns zero time if bucket has no offset
func (om *OffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionId uint16) (time.Time, error) {
	if bucket == "" {
		return time.Time{}, fmt.Errorf("bucket name cannot be empty")
	}

	query := fmt.Sprintf(`
        SELECT maxOrNull(last_processed_ts)
        FROM %s.%s
        WHERE bucketName = ? AND raftSessionId = ?
    `, om.database, clickhouse.TableOffsets)

	row := om.client.QueryRow(ctx, query, bucket, raftSessionId)

	var timestamp sql.NullTime
	err := row.Scan(&timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get offset for bucket %s raftSessionId %d: %w", bucket, raftSessionId, err)
	}

	// If no offset exists for this bucket/raftSession, maxOrNull() returns NULL
	// Use sql.NullTime so NULL is properly detected via the Valid field
	if !timestamp.Valid {
		return time.Time{}, nil
	}
	return timestamp.Time, nil
}
