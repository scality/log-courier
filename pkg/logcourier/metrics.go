package logcourier

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for log-courier, grouped by operation
// NOTE: No bucket labels are used to avoid high cardinality issues
type Metrics struct {
	General   GeneralMetrics
	Discovery DiscoveryMetrics
	Fetch     FetchMetrics
	Build     BuildMetrics
	Upload    UploadMetrics
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

// DiscoveryMetrics tracks batch discovery operations
type DiscoveryMetrics struct {
	// BatchesFound tracks total batches discovered during work discovery
	BatchesFound prometheus.Counter

	// BucketsWithLogging tracks number of buckets with pending logs
	BucketsWithLogging prometheus.Gauge

	// PendingBatches tracks number of batches waiting for processing
	PendingBatches prometheus.Gauge

	// Duration tracks time spent in discovery queries
	Duration prometheus.Histogram
}

// FetchMetrics tracks log fetching from ClickHouse
type FetchMetrics struct {
	// RecordsTotal tracks total records fetched from ClickHouse
	RecordsTotal prometheus.Counter

	// BatchSize tracks distribution of records per batch
	BatchSize prometheus.Histogram

	// Duration tracks time spent fetching logs
	Duration prometheus.Histogram
}

// BuildMetrics tracks log object building
type BuildMetrics struct {
	// ObjectsTotal tracks total log objects created
	ObjectsTotal prometheus.Counter

	// ObjectSizeBytes tracks distribution of object sizes
	ObjectSizeBytes prometheus.Histogram

	// Duration tracks time spent building log objects
	Duration prometheus.Histogram
}

// UploadMetrics tracks S3 upload operations
type UploadMetrics struct {
	// ObjectsTotal tracks total upload attempts
	ObjectsTotal *prometheus.CounterVec // labels: status (success/failed)

	// Duration tracks time spent uploading to S3
	Duration prometheus.Histogram
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

		Discovery: DiscoveryMetrics{
			BatchesFound: factory.NewCounter(
				prometheus.CounterOpts{
					Name: "log_courier_discovery_batches_found_total",
					Help: "Total number of log batches discovered during work discovery",
				},
			),
			BucketsWithLogging: factory.NewGauge(
				prometheus.GaugeOpts{
					Name: "log_courier_discovery_buckets_with_logging",
					Help: "Number of buckets with logging enabled and pending logs",
				},
			),
			PendingBatches: factory.NewGauge(
				prometheus.GaugeOpts{
					Name: "log_courier_discovery_pending_batches",
					Help: "Number of batches pending processing",
				},
			),
			Duration: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_discovery_duration_seconds",
					Help:    "Time spent discovering batches in ClickHouse",
					Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10}, // 10ms to 10s
				},
			),
		},

		Fetch: FetchMetrics{
			RecordsTotal: factory.NewCounter(
				prometheus.CounterOpts{
					Name: "log_courier_fetch_records_total",
					Help: "Total number of log records fetched from ClickHouse",
				},
			),
			BatchSize: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_fetch_batch_size_records",
					Help:    "Distribution of number of records fetched per batch",
					Buckets: prometheus.ExponentialBuckets(10, 2, 15), // 10, 20, 40, ..., up to ~163k
				},
			),
			Duration: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_fetch_duration_seconds",
					Help:    "Time spent fetching log records from ClickHouse",
					Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10}, // 10ms to 10s
				},
			),
		},

		Build: BuildMetrics{
			ObjectsTotal: factory.NewCounter(
				prometheus.CounterOpts{
					Name: "log_courier_build_objects_total",
					Help: "Total number of log objects built",
				},
			),
			ObjectSizeBytes: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_build_object_size_bytes",
					Help:    "Size in bytes of log objects built",
					Buckets: []float64{1024, 10240, 102400, 1048576, 10485760, 104857600}, // 1KB to 100MB
				},
			),
			Duration: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_build_duration_seconds",
					Help:    "Time spent building log objects",
					Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1}, // 1ms to 1s
				},
			),
		},

		Upload: UploadMetrics{
			ObjectsTotal: factory.NewCounterVec(
				prometheus.CounterOpts{
					Name: "log_courier_upload_objects_total",
					Help: "Total number of log object upload attempts",
				},
				[]string{"status"}, // status: success, failed
			),
			Duration: factory.NewHistogram(
				prometheus.HistogramOpts{
					Name:    "log_courier_upload_duration_seconds",
					Help:    "Time spent uploading log objects to S3",
					Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60}, // 100ms to 1min
				},
			),
		},
	}
}
