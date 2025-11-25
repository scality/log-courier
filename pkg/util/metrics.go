package util

import (
	"context"
	"errors"
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

// StartMetricsServerIfEnabled starts a metrics server.
//
// The server responds to queries to the URL path "/metrics", and returns the metrics registered
// in the default global registry.
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

	// Update server address to reflect actual listening address
	metricsServer.Addr = metricsListener.Addr().String()

	go func() {
		if err := metricsServer.Serve(metricsListener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "error", err)
		}
	}()

	logger.Info("metrics server started",
		"address", metricsListener.Addr().String())

	return metricsServer, nil
}
