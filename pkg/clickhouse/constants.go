package clickhouse

// DatabaseName is the ClickHouse database used for log storage
const DatabaseName = "logs"

// Table names
const (
	// TableAccessLogs is the main table storing log records (MergeTree)
	TableAccessLogs = "access_logs"

	// TableAccessLogsFederated is the distributed table across all shards
	TableAccessLogsFederated = "access_logs_federated"

	// TableOffsets is the table tracking processing offsets per bucket (MergeTree)
	TableOffsets = "offsets"

	// TableOffsetsFederated is the distributed table for offsets across all shards
	TableOffsetsFederated = "offsets_federated"
)
