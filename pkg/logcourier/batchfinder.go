package logcourier

import (
	"context"
	"fmt"

	"github.com/scality/log-courier/pkg/clickhouse"
)

// BatchFinder finds log batches ready for processing from ClickHouse
type BatchFinder struct {
	client           *clickhouse.Client
	database         string
	countThreshold   int
	timeThresholdSec int
}

// NewBatchFinder creates a new batch finder instance
func NewBatchFinder(client *clickhouse.Client, database string, countThreshold, timeThresholdSec int) *BatchFinder {
	return &BatchFinder{
		client:           client,
		database:         database,
		countThreshold:   countThreshold,
		timeThresholdSec: timeThresholdSec,
	}
}

// FindBatches finds log batches ready for processing
func (bf *BatchFinder) FindBatches(ctx context.Context) ([]LogBatch, error) {
	query := fmt.Sprintf(`
        WITH
            -- Find the most recent timestamp processed for each bucket
            bucket_offsets AS (
                SELECT
                    bucketName,
                    max(last_processed_ts) as last_processed_ts
                FROM %s.%s
                GROUP BY bucketName
            ),

            -- Find unprocessed logs for each bucket (process all buckets, partitioning will be added later - TODO: LOGC-8)
            new_logs_by_bucket AS (
                SELECT
                    l.bucketName,
                    count() AS new_log_count,
                    min(l.insertedAt) as min_ts,
                    max(l.insertedAt) as max_ts
                FROM %s.%s AS l
                LEFT JOIN bucket_offsets AS o ON l.bucketName = o.bucketName
                WHERE l.insertedAt > COALESCE(o.last_processed_ts, toDateTime64('1970-01-01 00:00:00', 3))
                GROUP BY l.bucketName
            )
        -- Select buckets ready for log batch processing
        SELECT
            bucketName,
            new_log_count,
            min_ts,
            max_ts
        FROM new_logs_by_bucket
        WHERE new_log_count >= ?
            OR min_ts <= now() - INTERVAL ? SECOND
    `, bf.database, clickhouse.TableOffsets, bf.database, clickhouse.TableAccessLogs)

	rows, err := bf.client.Query(ctx, query, bf.countThreshold, bf.timeThresholdSec)
	if err != nil {
		return nil, fmt.Errorf("batch finder query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batches []LogBatch
	for rows.Next() {
		var batch LogBatch

		err := rows.Scan(&batch.Bucket, &batch.LogCount, &batch.MinTimestamp, &batch.MaxTimestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log batch: %w", err)
		}

		batches = append(batches, batch)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating log batches: %w", err)
	}

	return batches, nil
}
