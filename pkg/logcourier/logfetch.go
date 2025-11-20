package logcourier

import (
	"context"
	"fmt"

	"github.com/scality/log-courier/pkg/clickhouse"
)

// LogFetcher fetches logs from ClickHouse
type LogFetcher struct {
	client        *clickhouse.Client
	database      string
	offsetManager OffsetManagerInterface
}

// NewLogFetcher creates a new log fetcher
func NewLogFetcher(client *clickhouse.Client, database string, offsetManager OffsetManagerInterface) *LogFetcher {
	return &LogFetcher{
		client:        client,
		database:      database,
		offsetManager: offsetManager,
	}
}

// FetchLogs fetches logs for a batch using triple composite offset filtering
func (lf *LogFetcher) FetchLogs(ctx context.Context, batch LogBatch) ([]LogRecord, error) {
	// Get current offset
	offset, err := lf.offsetManager.GetOffset(ctx, batch.Bucket, batch.RaftSessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get offset: %w", err)
	}

	// Build WHERE clause with triple composite offset
	whereClause := "WHERE bucketName = ? AND raftSessionID = ?"
	args := []interface{}{batch.Bucket, batch.RaftSessionID}

	// Add triple composite offset filter if offset exists
	if !offset.InsertedAt.IsZero() {
		whereClause += ` AND (
			insertedAt > ?
			OR (insertedAt = ? AND timestamp > ?)
			OR (insertedAt = ? AND timestamp = ? AND req_id > ?)
		)`
		args = append(args,
			offset.InsertedAt,
			offset.InsertedAt, offset.Timestamp,
			offset.InsertedAt, offset.Timestamp, offset.ReqID,
		)
	}

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
			raftSessionID,
			timestamp
		FROM %s.%s
		%s
		ORDER BY insertedAt ASC, timestamp ASC, req_id ASC
	`, lf.database, clickhouse.TableAccessLogs, whereClause)

	rows, err := lf.client.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var records []LogRecord
	for rows.Next() {
		var rec LogRecord
		if err := rows.ScanStruct(&rec); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		records = append(records, rec)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return records, nil
}
