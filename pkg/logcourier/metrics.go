package logcourier

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for log-courier, grouped by operation
type Metrics struct {
	General   GeneralMetrics
	Discovery DiscoveryMetrics
	Fetch     FetchMetrics
	Build     BuildMetrics
	Upload    UploadMetrics
}

// GeneralMetrics tracks general system state and errors
type GeneralMetrics struct {
	BatchesProcessed *prometheus.CounterVec // labels: status (success/failed_permanent/failed_transient)
	RecordsPermanentErrors prometheus.Counter
	BatchProcessingDuration prometheus.Histogram
	CyclesTotal prometheus.Counter
	CycleDuration prometheus.Histogram
}

// DiscoveryMetrics tracks batch discovery operations
type DiscoveryMetrics struct {
	BatchesFound prometheus.Counter
	BucketsPerDiscovery prometheus.Gauge
	Duration prometheus.Histogram
}

// FetchMetrics tracks log fetching from ClickHouse
type FetchMetrics struct {
	RecordsTotal prometheus.Counter
	RecordsPerBucket prometheus.Histogram
	Duration prometheus.Histogram
}

// BuildMetrics tracks log object building
type BuildMetrics struct {
	ObjectsTotal prometheus.Counter
	ObjectSizeBytes prometheus.Histogram
	Duration prometheus.Histogram
}

// UploadMetrics tracks S3 upload operations
type UploadMetrics struct {
	ObjectsTotal *prometheus.CounterVec // labels: status (success/failed)
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
		General:   newGeneralMetrics(factory),
		Discovery: newDiscoveryMetrics(factory),
		Fetch:     newFetchMetrics(factory),
		Build:     newBuildMetrics(factory),
		Upload:    newUploadMetrics(factory),
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

func newDiscoveryMetrics(factory promauto.Factory) DiscoveryMetrics {
	return DiscoveryMetrics{
		BatchesFound: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "log_courier_discovery_batches_found_total",
				Help: "Total number of log batches discovered during work discovery",
			},
		),
		BucketsPerDiscovery: factory.NewGauge(
			prometheus.GaugeOpts{
				Name: "log_courier_discovery_buckets_per_discovery",
				Help: "Number of buckets discovered",
			},
		),
		Duration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "log_courier_discovery_duration_seconds",
				Help:    "Time spent discovering batches in ClickHouse",
				Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 20}, // 100ms to 20s
			},
		),
	}
}

func newFetchMetrics(factory promauto.Factory) FetchMetrics {
	// Get max logs per bucket from config (default: 100000)
	maxLogsPerBucket := ConfigSpec.GetInt("consumer.max-logs-per-bucket")
	if maxLogsPerBucket <= 0 {
		maxLogsPerBucket = 100000 // fallback to default if config not loaded
	}

	// Generate 10 linear buckets
	promBuckets := prometheus.LinearBuckets(float64(maxLogsPerBucket)/10, float64(maxLogsPerBucket)/10, 10)
	// Cover 1 log per bucket
	if promBuckets[0] != 1.0 {
		promBuckets = append([]float64{1.0}, promBuckets...)
	}
	return FetchMetrics{
		RecordsTotal: factory.NewCounter(
			prometheus.CounterOpts{
				Name: "log_courier_fetch_records_total",
				Help: "Total number of log records fetched from ClickHouse",
			},
		),
		RecordsPerBucket: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "log_courier_fetch_records_per_bucket",
				Help:    "Distribution records fetched per bucket",
				Buckets: promBuckets,
			},
		),
		Duration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "log_courier_fetch_duration_seconds",
				Help:    "Time spent fetching log records from ClickHouse",
				Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 20}, // 100ms to 20s
			},
		),
	}
}

func newBuildMetrics(factory promauto.Factory) BuildMetrics {
	return BuildMetrics{
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
				// Buckets: 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 500MB, 1GB
				Buckets: []float64{1024, 10240, 102400, 1048576, 10485760, 104857600, 524288000, 1073741824},
			},
		),
		Duration: factory.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "log_courier_build_duration_seconds",
				Help:    "Time spent building log objects",
				Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1, 2}, // 1ms to 2s
			},
		),
	}
}

func newUploadMetrics(factory promauto.Factory) UploadMetrics {
	return UploadMetrics{
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
				Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5}, // 10ms to 5s
			},
		),
	}
}
