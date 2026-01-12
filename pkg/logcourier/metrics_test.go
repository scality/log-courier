package logcourier

import (
	"github.com/prometheus/client_golang/prometheus"
)

// NewTestMetrics creates metrics with a custom registry for testing.
// This avoids conflicts when multiple tests create metrics concurrently.
func NewTestMetrics() *Metrics {
	return NewMetricsWithRegistry(prometheus.NewRegistry())
}
