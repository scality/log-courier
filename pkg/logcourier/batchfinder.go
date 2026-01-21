package logcourier

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/scality/log-courier/pkg/clickhouse"
)

// BatchFinder finds log batches ready for processing from ClickHouse
type BatchFinder struct {
	client           *clickhouse.Client
	database         string
	countThreshold   int
	timeThresholdSec int
	maxBuckets       int
	logger           *slog.Logger
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
		logger:           slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	}
}

// FindBatches finds log batches ready for processing
//
//nolint:funlen // Function length is due to extensive SQL comments for readability
func (bf *BatchFinder) FindBatches(ctx context.Context) ([]LogBatch, error) {
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
                GLOBAL LEFT JOIN bucket_offsets AS o
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
        -- Results are ordered by min_ts (oldest first) to prioritize buckets with oldest logs.
        -- LIMIT ensures we only process maxBuckets per discovery cycle.
        SELECT bucketName, raftSessionID, new_log_count, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId
        FROM new_logs_by_bucket
        WHERE new_log_count >= ?
           OR min_ts <= now() - INTERVAL ? SECOND
        ORDER BY min_ts ASC
        LIMIT ?
    `, bf.database, clickhouse.TableOffsetsFederated, bf.database, clickhouse.TableAccessLogsFederated)

	rows, err := bf.client.Query(ctx, query, bf.countThreshold, bf.timeThresholdSec, bf.maxBuckets)
	if err != nil {
		return nil, fmt.Errorf("batch finder query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var batches []LogBatch
	batchKeys := make(map[string]int) // Track duplicate detection
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
			return nil, fmt.Errorf("failed to scan log batch: %w", err)
		}

		// Log each discovered batch for debugging
		logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
		logger.Info("BATCH_DISCOVERED",
			"bucket", batch.Bucket,
			"raftSessionID", batch.RaftSessionID,
			"logCount", batch.LogCount,
			"offsetInsertedAt", batch.LastProcessedOffset.InsertedAt,
			"offsetStartTime", batch.LastProcessedOffset.StartTime,
			"offsetReqID", batch.LastProcessedOffset.ReqID,
			"hasOffset", !batch.LastProcessedOffset.InsertedAt.IsZero())

		// Detect duplicate batches in same discovery cycle
		batchKey := fmt.Sprintf("%s-%d-%s-%s-%s",
			batch.Bucket,
			batch.RaftSessionID,
			batch.LastProcessedOffset.InsertedAt,
			batch.LastProcessedOffset.StartTime,
			batch.LastProcessedOffset.ReqID)
		batchKeys[batchKey]++
		if batchKeys[batchKey] > 1 {
			logger.Warn("DUPLICATE BATCH DETECTED IN FINDBATCHES",
				"bucket", batch.Bucket,
				"raftSessionID", batch.RaftSessionID,
				"offset", batch.LastProcessedOffset.InsertedAt,
				"duplicateNumber", batchKeys[batchKey])
		}

		batches = append(batches, batch)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating log batches: %w", err)
	}

	// Log discovery summary
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logger.Info("BATCH_DISCOVERY_COMPLETE",
		"totalBatches", len(batches),
		"countThreshold", bf.countThreshold,
		"timeThresholdSec", bf.timeThresholdSec)

	return batches, nil
}
