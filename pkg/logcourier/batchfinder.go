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
            -- Find the maximum insertedAt for each bucket/raftSessionID
            max_offsets AS (
                SELECT
                    bucketName,
                    raftSessionID,
                    max(lastProcessedInsertedAt) as maxInsertedAt
                FROM %s.offsets
                GROUP BY bucketName, raftSessionID
            ),

            -- Get the full row for each maximum offset
            bucket_offsets AS (
                SELECT
                    o.bucketName,
                    o.raftSessionID,
                    o.lastProcessedInsertedAt,
                    o.lastProcessedTimestamp,
                    o.lastProcessedReqId
                FROM %s.offsets o
                INNER JOIN max_offsets m
                    ON o.bucketName = m.bucketName
                    AND o.raftSessionID = m.raftSessionID
                    AND o.lastProcessedInsertedAt = m.maxInsertedAt
            ),

            -- Find unprocessed logs for each bucket
            new_logs_by_bucket AS (
                SELECT
                    l.bucketName,
                    l.raftSessionID,
                    count() AS new_log_count,
                    min(l.insertedAt) as min_ts,
                    max(l.insertedAt) as max_ts
                FROM %s.access_logs AS l
                LEFT JOIN bucket_offsets AS o
                    ON l.bucketName = o.bucketName
                    AND l.raftSessionID = o.raftSessionID
                WHERE
                    -- Triple composite offset comparison
                    l.insertedAt > COALESCE(o.lastProcessedInsertedAt, toDateTime('1970-01-01 00:00:00'))
                    OR (
                        l.insertedAt = o.lastProcessedInsertedAt
                        AND l.timestamp > COALESCE(o.lastProcessedTimestamp, toDateTime64('1970-01-01 00:00:00', 3))
                    )
                    OR (
                        l.insertedAt = o.lastProcessedInsertedAt
                        AND l.timestamp = o.lastProcessedTimestamp
                        AND l.req_id > COALESCE(o.lastProcessedReqId, '')
                    )
                GROUP BY l.bucketName, l.raftSessionID
            )
        -- Main query: Select buckets ready for batch processing
        SELECT bucketName, raftSessionID, new_log_count, min_ts, max_ts
        FROM new_logs_by_bucket
        WHERE new_log_count >= ?
           OR min_ts <= now() - INTERVAL ? SECOND
        ORDER BY min_ts ASC
    `, bf.database, bf.database, bf.database)

	rows, err := bf.client.Query(ctx, query, bf.countThreshold, bf.timeThresholdSec)
	if err != nil {
		return nil, fmt.Errorf("batch finder query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batches []LogBatch
	for rows.Next() {
		var batch LogBatch

		err := rows.Scan(&batch.Bucket, &batch.RaftSessionID, &batch.LogCount, &batch.MinTimestamp, &batch.MaxTimestamp)
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
