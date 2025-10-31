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

// FetchLogs fetches logs for a log batch
// Returns logs sorted by timestamp (ascending) for proper chronological ordering
func (lf *LogFetcher) FetchLogs(ctx context.Context, batch LogBatch) ([]LogRecord, error) {
	query := fmt.Sprintf(`
		SELECT
			bucketOwner,
			bucketName,
			startTime,
			clientIP,
			requester,
			req_id,
			action,
			objectKey,
			httpURL,
			httpCode,
			errorCode,
			bytesSent,
			contentLength,
			elapsed_ms,
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
			insertedAt,
			loggingTargetBucket,
			loggingTargetPrefix,
			timestamp
		FROM %s.%s
		WHERE bucketName = ?
		  AND insertedAt >= ?
		  AND insertedAt <= ?
		ORDER BY timestamp ASC
	`, lf.database, clickhouse.TableAccessLogs)

	rows, err := lf.client.Query(ctx, query, batch.Bucket, batch.MinTimestamp, batch.MaxTimestamp)
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
