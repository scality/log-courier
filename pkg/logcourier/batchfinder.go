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
	maxBuckets       int
}

// BucketCursor represents a position in paginated bucket discovery
type BucketCursor struct {
	Bucket        string
	RaftSessionID uint16
}

// BucketName returns the bucket name, or empty string if cursor is nil
func (c *BucketCursor) BucketName() string {
	if c == nil {
		return ""
	}
	return c.Bucket
}

// SessionID returns the raft session ID, or 0 if cursor is nil
func (c *BucketCursor) SessionID() uint16 {
	if c == nil {
		return 0
	}
	return c.RaftSessionID
}

// NewBatchFinder creates a new batch finder instance
func NewBatchFinder(
	client *clickhouse.Client,
	database string,
	countThreshold int,
	timeThresholdSec int,
	maxBuckets int,
) *BatchFinder {
	return &BatchFinder{
		client:           client,
		database:         database,
		countThreshold:   countThreshold,
		timeThresholdSec: timeThresholdSec,
		maxBuckets:       maxBuckets,
	}
}

// FindBatches finds log batches ready for processing
// It uses cursor-based pagination for buckets.
// Given a cursor, it considers buckets after the cursor position.
// Pass nil cursor to start from the beginning.
// Returns the batches ready for processing and a cursor for the next page.
// Returns nil cursor when no more results are found (cycle complete).
//
//nolint:funlen // Function length is due to extensive SQL comments for readability
func (bf *BatchFinder) FindBatches(ctx context.Context, cursor *BucketCursor) ([]LogBatch, *BucketCursor, error) {
	// Use zero values when cursor is nil (start from beginning)
	var cursorBucket string
	var cursorRaftSessionID uint16
	if cursor != nil {
		cursorBucket = cursor.Bucket
		cursorRaftSessionID = cursor.RaftSessionID
	}
	query := fmt.Sprintf(`
        WITH
            -- CTE 1: bucket_offsets
            -- Purpose: Get the most recent offset for each bucket
            --
            -- Uses ROW_NUMBER() window function to rank offsets by the composite offset keys
            -- in descending order and select the first row.
            --
            -- PARTITION BY ensures we get one offset per bucket
            bucket_offsets AS (
                SELECT
                    bucketName,
                    raftSessionID,
                    lastProcessedInsertedAt,
                    lastProcessedStartTime,
                    lastProcessedReqId
                FROM (
                    SELECT
                        bucketName,
                        raftSessionID,
                        lastProcessedInsertedAt,
                        lastProcessedStartTime,
                        lastProcessedReqId,
                        ROW_NUMBER() OVER (
                            PARTITION BY bucketName, raftSessionID
                            ORDER BY lastProcessedInsertedAt DESC, lastProcessedStartTime DESC, lastProcessedReqId DESC
                        ) as rn
                    FROM %s.%s
                ) offsets_ordered
                WHERE rn = 1
            ),

            -- CTE 2: new_logs_by_bucket
            -- Purpose: Count unprocessed logs for each bucket
            --
            -- Joins access_logs with bucket_offsets to find logs that haven't been processed yet.
            -- A log is considered unprocessed if its (insertedAt, startTime, reqID) is greater
            -- than the stored offset using lexicographic comparison:
            --   1. insertedAt > offset.insertedAt, OR
            --   2. insertedAt = offset.insertedAt AND startTime > offset.startTime, OR
            --   3. insertedAt = offset.insertedAt AND startTime = offset.startTime AND reqID > offset.reqID
            --
            -- For buckets with no offset, LEFT JOIN returns NULL values which are handled explicitly in WHERE clause.
            new_logs_by_bucket AS (
                SELECT
                    l.bucketName,
                    l.raftSessionID,
                    count() AS new_log_count,
                    min(l.insertedAt) as min_ts,
                    o.lastProcessedInsertedAt,
                    o.lastProcessedStartTime,
                    o.lastProcessedReqId
                FROM %s.%s AS l
                LEFT JOIN bucket_offsets AS o
                    ON l.bucketName = o.bucketName
                    AND l.raftSessionID = o.raftSessionID
                WHERE
                    -- Triple composite offset comparison: log > offset
                    -- When no offset exists (NULL from LEFT JOIN), include all logs
                    o.lastProcessedInsertedAt IS NULL
                    OR l.insertedAt > o.lastProcessedInsertedAt
                    OR (
                        l.insertedAt = o.lastProcessedInsertedAt
                        AND l.startTime > o.lastProcessedStartTime
                    )
                    OR (
                        l.insertedAt = o.lastProcessedInsertedAt
                        AND l.startTime = o.lastProcessedStartTime
                        AND l.req_id > o.lastProcessedReqId
                    )
                GROUP BY l.bucketName, l.raftSessionID, o.lastProcessedInsertedAt, o.lastProcessedStartTime, o.lastProcessedReqId
            )
        -- Main query: Select buckets that are ready for processing
        --
        -- A bucket is ready if either:
        --   1. It has at least countThreshold unprocessed logs (volume condition), OR
        --   2. Its oldest unprocessed log is older than timeThresholdSec (age condition)
        --
        -- Results are ordered by (bucketName, raftSessionID) for cursor-based pagination.
        -- The cursor filter ensures we only return buckets after the current position.
        -- LIMIT ensures we only process maxBuckets per discovery cycle.
        SELECT bucketName, raftSessionID, new_log_count, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId
        FROM new_logs_by_bucket
        WHERE (new_log_count >= ? OR min_ts <= now() - INTERVAL ? SECOND)
          AND (bucketName, raftSessionID) > (?, ?)
        ORDER BY bucketName ASC, raftSessionID ASC
        LIMIT ?
    `, bf.database, clickhouse.TableOffsets, bf.database, clickhouse.TableAccessLogsFederated)

	rows, err := bf.client.Query(ctx, query, bf.countThreshold, bf.timeThresholdSec, cursorBucket, cursorRaftSessionID, bf.maxBuckets)
	if err != nil {
		return nil, nil, fmt.Errorf("batch finder query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batches []LogBatch
	for rows.Next() {
		var batch LogBatch
		err := rows.Scan(
			&batch.Bucket,
			&batch.RaftSessionID,
			&batch.LogCount,
			&batch.LastProcessedOffset.InsertedAt,
			&batch.LastProcessedOffset.StartTime,
			&batch.LastProcessedOffset.ReqID,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to scan log batch: %w", err)
		}
		batches = append(batches, batch)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("error iterating log batches: %w", err)
	}

	if len(batches) == 0 {
		return batches, nil, nil
	}

	last := batches[len(batches)-1]
	nextCursor := &BucketCursor{Bucket: last.Bucket, RaftSessionID: last.RaftSessionID}

	return batches, nextCursor, nil
}
