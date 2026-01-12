package logcourier

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for log-courier, grouped by operation
type Metrics struct {
	General GeneralMetrics
}

// GeneralMetrics tracks general system state and errors
type GeneralMetrics struct {
	BatchesProcessed *prometheus.CounterVec // labels: status (success/failed_permanent/failed_transient)
	RecordsPermanentErrors prometheus.Counter
	BatchProcessingDuration prometheus.Histogram
	CyclesTotal prometheus.Counter
	CycleDuration prometheus.Histogram
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
		General: newGeneralMetrics(factory),
	}
}

func newGeneralMetrics(factory promauto.Factory) GeneralMetrics {
	return GeneralMetrics{
		BatchesProcessed: factory.NewCounterVec(
			prometheus.CounterOpts{
				Name: "log_courier_batches_processed_total",
				Help: "Total number of log batches processed",
			},
			[]string{"status"}, // status: success, failed_permanent, failed_transient
		),
		RecordsPermanentErrors: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "log_courier_records_in_permanent_errors_total",
				Help: "Total number of records that may be lost after TTL if error is not fixed",
			},
		),
		BatchProcessingDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "log_courier_batch_processing_duration_seconds",
				Help:    "End-to-end batch processing time (fetch + build + upload)",
				Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300}, // 500ms to 5min
			},
		),
		CyclesTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "log_courier_cycles_total",
				Help: "Total number of processing cycles completed",
			},
		),
		CycleDuration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "log_courier_cycle_duration_seconds",
				Help:    "Time spent in each processing cycle (discovery + processing including retries)",
				Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 120, 300}, // 500ms to 5min
			},
		),
	}
}
