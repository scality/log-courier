package logcourier

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for log-courier, grouped by operation
// NOTE: No bucket labels are used to avoid high cardinality issues
type Metrics struct {
	General GeneralMetrics
}

// GeneralMetrics tracks general system state and errors
type GeneralMetrics struct {
	// BatchesProcessed tracks total batches processed with status
	BatchesProcessed *prometheus.CounterVec // labels: status (success/failed_permanent/failed_transient)

	// RecordsLost tracks records lost due to permanent errors
	RecordsLost prometheus.Counter

	// BatchProcessingDuration tracks end-to-end batch processing time
	BatchProcessingDuration prometheus.Histogram
}

// NewMetrics creates and registers all Prometheus metrics
func NewMetrics() *Metrics {
	return NewMetricsWithRegistry(prometheus.DefaultRegisterer)
}

// NewMetricsWithRegistry creates metrics with a custom registry
// This is useful for testing to avoid conflicts with the default registry
func NewMetricsWithRegistry(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		General: GeneralMetrics{
			BatchesProcessed: factory.NewCounterVec(
				prometheus.CounterOpts{
					Name: "log_courier_batches_processed_total",
					Help: "Total number of log batches processed",
				},
				[]string{"status"}, // status: success, failed_permanent, failed_transient
			),
			RecordsLost: factory.NewCounter(
				prometheus.CounterOpts{
					Name: "log_courier_records_lost_total",
					Help: "Total number of log records lost due to permanent errors",
				},
			),
			BatchProcessingDuration: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_batch_processing_duration_seconds",
					Help:    "End-to-end batch processing time (fetch + build + upload)",
					Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300}, // 100ms to 5min
				},
			),
		},
	}
}
