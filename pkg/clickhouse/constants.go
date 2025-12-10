package clickhouse

// DatabaseName is the ClickHouse database used for log storage
const DatabaseName = "logs"

// Table names
const (
	// TableAccessLogsIngest is the ingest table for incoming log records (Null engine)
	TableAccessLogsIngest = "access_logs_ingest"

	// TableAccessLogs is the main table storing log records (MergeTree)
	TableAccessLogs = "access_logs"

	// TableAccessLogsFederated is the distributed table for writes (through buffer)
	TableAccessLogsFederated = "access_logs_federated"

	// TableAccessLogsFederatedQuery is the distributed table for queries (bypasses buffer)
	TableAccessLogsFederatedQuery = "access_logs_federated_query"

	// TableOffsets is the table tracking processing offsets per bucket (MergeTree)
	TableOffsets = "offsets"

	// ViewAccessLogsIngestMV is the materialized view filtering loggingEnabled=true records
	ViewAccessLogsIngestMV = "access_logs_ingest_mv"
)
