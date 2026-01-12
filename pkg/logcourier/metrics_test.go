package logcourier_test

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scality/log-courier/pkg/logcourier"
)

// NewTestMetrics creates metrics with a custom registry for testing.
// This avoids conflicts when multiple tests create metrics concurrently.
func NewTestMetrics() *logcourier.Metrics {
	return logcourier.NewMetricsWithRegistry(prometheus.NewRegistry())
}
