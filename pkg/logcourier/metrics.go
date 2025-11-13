package logcourier

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for log-courier
//
//nolint:gochecknoglobals // global metrics registry is intentional
var Metrics = struct {
	BatchesDiscovered     prometheus.Counter
	BatchesProcessed      *prometheus.CounterVec
	LogsProcessed         prometheus.Counter
	S3UploadDuration      prometheus.Histogram
	S3UploadSize          prometheus.Histogram
	ClickHouseQueryDuration prometheus.Histogram
	OldestUnprocessedLogAge prometheus.Gauge
}{
	BatchesDiscovered: promauto.NewCounter(prometheus.CounterOpts{
		Name: "log_courier_batches_discovered_total",
		Help: "Total number of log batches discovered",
	}),

	BatchesProcessed: promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "log_courier_batches_processed_total",
		Help: "Total number of log batches processed",
	}, []string{"status"}), // status: success, error

	LogsProcessed: promauto.NewCounter(prometheus.CounterOpts{
		Name: "log_courier_logs_processed_total",
		Help: "Total number of log records processed",
	}),

	S3UploadDuration: promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "log_courier_s3_upload_duration_seconds",
		Help:    "Duration of S3 uploads in seconds",
		Buckets: prometheus.DefBuckets,
	}),

	S3UploadSize: promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "log_courier_s3_upload_size_bytes",
		Help:    "Size of uploaded log objects in bytes",
		Buckets: prometheus.ExponentialBuckets(1024, 2, 15), // 1KB to 16MB
	}),

	ClickHouseQueryDuration: promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "log_courier_clickhouse_query_duration_seconds",
		Help:    "Duration of ClickHouse queries in seconds",
		Buckets: prometheus.DefBuckets,
	}),

	OldestUnprocessedLogAge: promauto.NewGauge(prometheus.GaugeOpts{
		Name: "log_courier_oldest_unprocessed_log_age_seconds",
		Help: "Age of the oldest unprocessed log across all buckets in seconds",
	}),
}
