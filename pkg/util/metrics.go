package util

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type promhttpErrorLogger struct {
	promhttp.Logger

	baseLogger *slog.Logger
}

func (logger *promhttpErrorLogger) Println(v ...interface{}) {
	logger.baseLogger.Error("error during handling of metrics request", "errorArgs", v)
}

// StartMetricsServerIfEnabled starts a metrics server configured under the configuration prefix
// `confPrefix` of the configuration spec `configSpec`:
//
//   - If the value of "enabled" under this prefix is true, it starts the server and listens on
//     the interface specified by "listen-address" and port "listen-port" under the prefix.
//
//   - If the value of "enabled" is false, it just returns nil
//
// The server responds to queries to the URL path "/metrics", and returns the metrics registered
// in the default global registry (prometheus.DefaultRegisterer), with output format based on
// content-type negotiation (in Prometheus text format by default).
//
// If non-nil, the returned server should be eventually closed with Close() or Shutdown().
func StartMetricsServerIfEnabled(configSpec ConfigSpec, confPrefix string,
	logger *slog.Logger) (*http.Server, error) {
	serverEnabled := configSpec.GetBool(fmt.Sprintf("%s.enabled", confPrefix))
	if !serverEnabled {
		return nil, nil
	}
	serverAddress := configSpec.GetString(fmt.Sprintf("%s.listen-address", confPrefix))
	serverPort := configSpec.GetInt(fmt.Sprintf("%s.listen-port", confPrefix))

	logger.Debug("starting metrics server",
		"address", serverAddress, "port", serverPort)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		ErrorLog: &promhttpErrorLogger{baseLogger: logger},
	}))

	metricsServer := &http.Server{
		Handler:           mux,
		Addr:              fmt.Sprintf("%s:%d", serverAddress, serverPort),
		ReadHeaderTimeout: 10 * time.Second,
	}

	listenConfig := &net.ListenConfig{}
	metricsListener, err := listenConfig.Listen(context.Background(), "tcp", metricsServer.Addr)
	if err != nil {
		logger.Error("metrics server: error listening on specified address",
			"address", metricsServer.Addr, "error", err)
		return nil, err
	}

	go func() {
		defer func() {
			if closeErr := metricsListener.Close(); closeErr != nil {
				logger.Error("failed to close metrics listener", "error", closeErr)
			}
		}()

		_ = metricsServer.Serve(metricsListener)
	}()

	logger.Info("metrics server started",
		"address", serverAddress, "port", serverPort)

	return metricsServer, nil
}
