package logcourier

import "github.com/scality/log-courier/pkg/util"

// ConfigSpec defines all configuration items for log-courier
//
//nolint:gochecknoglobals // global config spec is intentional
var ConfigSpec = util.ConfigSpec{
	// General
	"log-level": util.ConfigVarSpec{
		Help:         "Log level (error|warn|info|debug)",
		DefaultValue: "info",
		EnvVar:       "LOG_COURIER_LOG_LEVEL",
	},
}
