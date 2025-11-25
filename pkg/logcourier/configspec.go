package logcourier

import "github.com/scality/log-courier/pkg/util"

// ConfigSpec defines all configuration items for log-courier
//
//nolint:gochecknoglobals // global config spec is intentional
var ConfigSpec = util.ConfigSpec{
	// ClickHouse connection
	"clickhouse.url": util.ConfigVarSpec{
		Help:         "ClickHouse connection URL(s) - single host or comma-separated list (e.g., 'host1:9000,host2:9000')",
		DefaultValue: "localhost:9002",
		EnvVar:       "LOG_COURIER_CLICKHOUSE_URL",
		ParseFunc: func(rawValue any) (any, error) {
			if str, ok := rawValue.(string); ok {
				return util.ParseCommaSeparatedHosts(str), nil
			}
			return rawValue, nil
		},
	},
	"clickhouse.username": util.ConfigVarSpec{
		Help:         "ClickHouse username",
		DefaultValue: "default",
		EnvVar:       "LOG_COURIER_CLICKHOUSE_USERNAME",
	},
	"clickhouse.password": util.ConfigVarSpec{
		Help:         "ClickHouse password",
		DefaultValue: "",
		EnvVar:       "LOG_COURIER_CLICKHOUSE_PASSWORD",
	},
	"clickhouse.timeout-seconds": util.ConfigVarSpec{
		Help:         "ClickHouse query timeout in seconds",
		DefaultValue: 30,
		EnvVar:       "LOG_COURIER_CLICKHOUSE_TIMEOUT_SECONDS",
	},

	// Consumer thresholds
	"consumer.count-threshold": util.ConfigVarSpec{
		Help:         "Minimum number of unprocessed logs to trigger batch processing",
		DefaultValue: 1000,
		EnvVar:       "LOG_COURIER_CONSUMER_COUNT_THRESHOLD",
	},
	"consumer.time-threshold-seconds": util.ConfigVarSpec{
		Help:         "Age in seconds after which logs should be processed regardless of count",
		DefaultValue: 900,
		EnvVar:       "LOG_COURIER_CONSUMER_TIME_THRESHOLD_SECONDS",
	},
	"consumer.discovery-interval-seconds": util.ConfigVarSpec{
		Help:         "Interval in seconds between work discovery runs",
		DefaultValue: 60,
		EnvVar:       "LOG_COURIER_CONSUMER_DISCOVERY_INTERVAL_SECONDS",
	},
	"consumer.discovery-interval-jitter-factor": util.ConfigVarSpec{
		Help:         "Jitter factor for discovery interval (0.0 to 1.0, where 0 is no jitter and 1.0 is up to 100% jitter)",
		DefaultValue: 0.1,
		EnvVar:       "LOG_COURIER_CONSUMER_DISCOVERY_INTERVAL_JITTER_FACTOR",
	},
	"consumer.num-workers": util.ConfigVarSpec{
		Help:         "Number of parallel workers for batch processing",
		DefaultValue: 10,
		EnvVar:       "LOG_COURIER_CONSUMER_NUM_WORKERS",
	},

	// Retry configuration
	"retry.max-retries": util.ConfigVarSpec{
		Help:         "Maximum number of retry attempts for failed batch processing",
		DefaultValue: 5,
		EnvVar:       "LOG_COURIER_RETRY_MAX_RETRIES",
	},
	"retry.initial-backoff-seconds": util.ConfigVarSpec{
		Help:         "Initial backoff duration in seconds for retry attempts",
		DefaultValue: 1,
		EnvVar:       "LOG_COURIER_RETRY_INITIAL_BACKOFF_SECONDS",
	},
	"retry.max-backoff-seconds": util.ConfigVarSpec{
		Help:         "Maximum backoff duration in seconds for retry attempts",
		DefaultValue: 60,
		EnvVar:       "LOG_COURIER_RETRY_MAX_BACKOFF_SECONDS",
	},
	"retry.backoff-jitter-factor": util.ConfigVarSpec{
		Help:         "Jitter factor for backoff (0.0 to 1.0, where 0 is no jitter and 1.0 is up to 100% jitter)",
		DefaultValue: 0.3,
		EnvVar:       "LOG_COURIER_RETRY_BACKOFF_JITTER_FACTOR",
	},

	// S3 configuration
	"s3.endpoint": util.ConfigVarSpec{
		Help:         "S3 endpoint URL",
		DefaultValue: "127.0.0.1:8000",
		EnvVar:       "S3_ENDPOINT",
	},
	"s3.access-key-id": util.ConfigVarSpec{
		Help:         "S3 access key ID",
		DefaultValue: "",
		EnvVar:       "S3_ACCESS_KEY_ID",
	},
	"s3.secret-access-key": util.ConfigVarSpec{
		Help:         "S3 secret access key",
		DefaultValue: "",
		EnvVar:       "S3_SECRET_ACCESS_KEY",
	},
	"s3.max-retry-attempts": util.ConfigVarSpec{
		Help:         "Maximum number of retry attempts for S3 operations (including initial request)",
		DefaultValue: 3,
		EnvVar:       "S3_MAX_RETRY_ATTEMPTS",
	},
	"s3.max-backoff-delay-seconds": util.ConfigVarSpec{
		Help:         "Maximum backoff delay in seconds between S3 retry attempts",
		DefaultValue: 20,
		EnvVar:       "S3_MAX_BACKOFF_DELAY_SECONDS",
	},

	// Metrics server
	"metrics-server.enabled": util.ConfigVarSpec{
		Help:         "Enable Prometheus metrics server",
		DefaultValue: true,
		EnvVar:       "LOG_COURIER_METRICS_SERVER_ENABLED",
	},
	"metrics-server.listen-address": util.ConfigVarSpec{
		Help:         "Metrics server listening address (examples: \"\", \"127.0.0.1\", \"[::1]\") - an empty value causes the metrics server to listen to all addresses",
		DefaultValue: "127.0.0.1",
		EnvVar:       "LOG_COURIER_METRICS_SERVER_LISTEN_ADDRESS",
	},
	"metrics-server.listen-port": util.ConfigVarSpec{
		Help:         "Metrics server listening port",
		DefaultValue: 9090,
		EnvVar:       "LOG_COURIER_METRICS_SERVER_LISTEN_PORT",
	},

	// General
	"log-level": util.ConfigVarSpec{
		Help:         "Log level (error|warn|info|debug)",
		DefaultValue: "info",
		EnvVar:       "LOG_COURIER_LOG_LEVEL",
	},
	"shutdown-timeout-seconds": util.ConfigVarSpec{
		Help:         "Maximum time to wait for graceful shutdown in seconds",
		DefaultValue: 30,
		EnvVar:       "LOG_COURIER_SHUTDOWN_TIMEOUT_SECONDS",
	},
}
