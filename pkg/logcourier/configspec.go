package logcourier

import "github.com/scality/log-courier/pkg/util"

// ConfigSpec defines all configuration items for log-courier
//
//nolint:gochecknoglobals // global config spec is intentional
var ConfigSpec = util.ConfigSpec{
	// ClickHouse connection
	"clickhouse.url": util.ConfigVarSpec{
		Help:         "ClickHouse connection URL",
		DefaultValue: "localhost:9000",
		EnvVar:       "LOG_COURIER_CLICKHOUSE_URL",
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

	// General
	"log-level": util.ConfigVarSpec{
		Help:         "Log level (error|warn|info|debug)",
		DefaultValue: "info",
		EnvVar:       "LOG_COURIER_LOG_LEVEL",
	},
}
