package logcourier

import (
	"context"
	"fmt"

	"github.com/scality/log-courier/pkg/clickhouse"
)

// LogFetcher fetches logs from ClickHouse
type LogFetcher struct {
	client   *clickhouse.Client
	database string
}

// NewLogFetcher creates a new log fetcher
func NewLogFetcher(client *clickhouse.Client, database string) *LogFetcher {
	return &LogFetcher{
		client:   client,
		database: database,
	}
}

// FetchLogs fetches logs for a batch
// Returns logs sorted by insertedAt, timestamp, req_id
// LogBuilder will re-sort by timestamp for S3 file ordering.
// Uses composite filter to fetch only logs after LastProcessedOffset.
func (lf *LogFetcher) FetchLogs(ctx context.Context, batch LogBatch) ([]LogRecord, error) {
	query := fmt.Sprintf(`
		SELECT
			bucketOwner,
			bucketName,
			startTime,
			clientIP,
			requester,
			req_id,
			operation,
			objectKey,
			requestURI,
			httpCode,
			errorCode,
			bytesSent,
			objectSize,
			totalTime,
			turnAroundTime,
			referer,
			userAgent,
			versionId,
			signatureVersion,
			cipherSuite,
			authenticationType,
			hostHeader,
			tlsVersion,
			aclRequired,
			timestamp,
			insertedAt,
			loggingTargetBucket,
			loggingTargetPrefix,
			raftSessionID
		FROM %s.%s
		WHERE bucketName = ?
		  AND raftSessionID = ?
		  AND (
		      insertedAt > ?
		      OR (insertedAt = ? AND timestamp > ?)
		      OR (insertedAt = ? AND timestamp = ? AND req_id > ?)
		  )
		ORDER BY insertedAt ASC, timestamp ASC, req_id ASC
	`, lf.database, clickhouse.TableAccessLogs)

	rows, err := lf.client.Query(ctx, query,
		batch.Bucket,
		batch.RaftSessionID,
		batch.LastProcessedOffset.InsertedAt,
		batch.LastProcessedOffset.InsertedAt, batch.LastProcessedOffset.Timestamp,
		batch.LastProcessedOffset.InsertedAt, batch.LastProcessedOffset.Timestamp, batch.LastProcessedOffset.ReqID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs: %w", err)
	}
	defer func() {
		_ = rows.Close()
	}()

	var records []LogRecord
	for rows.Next() {
		var rec LogRecord
		if err := rows.ScanStruct(&rec); err != nil {
			return nil, fmt.Errorf("failed to scan log record: %w", err)
		}
		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating log records: %w", err)
	}

	return records, nil
}
