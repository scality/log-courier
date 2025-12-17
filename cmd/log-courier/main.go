package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/sys/unix"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/util"
)

func main() {
	os.Exit(run())
}

// buildProcessorConfig creates processor config from ConfigSpec
func buildProcessorConfig(logger *slog.Logger) logcourier.Config {
	return logcourier.Config{
		ClickHouseHosts:               logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
		ClickHouseUsername:            logcourier.ConfigSpec.GetString("clickhouse.username"),
		ClickHousePassword:            logcourier.ConfigSpec.GetString("clickhouse.password"),
		ClickHouseTimeout:             time.Duration(logcourier.ConfigSpec.GetInt("clickhouse.timeout-seconds")) * time.Second,
		CountThreshold:                logcourier.ConfigSpec.GetInt("consumer.count-threshold"),
		TimeThresholdSec:              logcourier.ConfigSpec.GetInt("consumer.time-threshold-seconds"),
		MinDiscoveryInterval:          time.Duration(logcourier.ConfigSpec.GetInt("consumer.min-discovery-interval-seconds")) * time.Second,
		MaxDiscoveryInterval:          time.Duration(logcourier.ConfigSpec.GetInt("consumer.max-discovery-interval-seconds")) * time.Second,
		DiscoveryIntervalJitterFactor: logcourier.ConfigSpec.GetFloat64("consumer.discovery-interval-jitter-factor"),
		NumWorkers:                    logcourier.ConfigSpec.GetInt("consumer.num-workers"),
		MaxBucketsPerDiscovery:        logcourier.ConfigSpec.GetInt("consumer.max-buckets-per-discovery"),
		MaxLogsPerBucket:              logcourier.ConfigSpec.GetInt("consumer.max-logs-per-bucket"),
		MaxRetries:                    logcourier.ConfigSpec.GetInt("retry.max-retries"),
		InitialBackoff:                time.Duration(logcourier.ConfigSpec.GetInt("retry.initial-backoff-seconds")) * time.Second,
		MaxBackoff:                    time.Duration(logcourier.ConfigSpec.GetInt("retry.max-backoff-seconds")) * time.Second,
		BackoffJitterFactor:           logcourier.ConfigSpec.GetFloat64("retry.backoff-jitter-factor"),
		UploadOperationTimeout:        time.Duration(logcourier.ConfigSpec.GetInt("timeout.upload-operation-seconds")) * time.Second,
		CommitOperationTimeout:        time.Duration(logcourier.ConfigSpec.GetInt("timeout.commit-operation-seconds")) * time.Second,
		S3Endpoint:                    logcourier.ConfigSpec.GetString("s3.endpoint"),
		S3AccessKeyID:                 logcourier.ConfigSpec.GetString("s3.access-key-id"),
		S3SecretAccessKey:             logcourier.ConfigSpec.GetString("s3.secret-access-key"),
		Logger:                        logger,
	}
}

// waitForShutdown waits for shutdown signal or processor error, returns exit code
func waitForShutdown(cancel context.CancelFunc, logger *slog.Logger,
	errChan <-chan error, signalsChan <-chan os.Signal, shutdownTimeout time.Duration) int {
	select {
	case sig := <-signalsChan:
		logger.Info("signal received", "signal", sig)
		cancel()

		// Wait for processor to stop gracefully (with timeout)
		shutdownTimer := time.NewTimer(shutdownTimeout)
		defer shutdownTimer.Stop()

		select {
		case <-shutdownTimer.C:
			logger.Warn("shutdown timeout exceeded, forcing exit")
			return 1
		case err := <-errChan:
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("processor stopped with error", "error", err)
				return 1
			}
		}

	case err := <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("processor error", "error", err)
			return 1
		}
	}

	return 0
}

func run() int {
	// Add command-line flags
	logcourier.ConfigSpec.AddFlag(pflag.CommandLine, "log-level", "log-level")

	configFileFlag := pflag.String("config-file", "", "Path to configuration file")
	pflag.Parse()

	// Load configuration
	configFile := *configFileFlag
	if configFile == "" {
		configFile = os.Getenv("LOG_COURIER_CONFIG_FILE")
	}

	err := logcourier.ConfigSpec.LoadConfiguration(configFile, "", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		pflag.Usage()
		return 2
	}

	// Validate configuration
	err = logcourier.ValidateConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration validation error: %v\n", err)
		return 2
	}

	// Set up logger
	logLevel := util.ParseLogLevel(logcourier.ConfigSpec.GetString("log-level"))
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	// Get shutdown timeout from config
	shutdownTimeout := time.Duration(logcourier.ConfigSpec.GetInt("shutdown-timeout-seconds")) * time.Second

	// Create processor
	ctx := context.Background()
	processorCfg := buildProcessorConfig(logger)

	processor, err := logcourier.NewProcessor(ctx, processorCfg)
	if err != nil {
		logger.Error("failed to create processor", "error", err)
		return 1
	}
	defer func() {
		if closeErr := processor.Close(); closeErr != nil {
			logger.Error("failed to close processor", "error", closeErr)
		}
	}()

	// Start metrics server
	metricsServer, err := util.StartMetricsServerIfEnabled(
		logcourier.ConfigSpec, "metrics-server", logger)
	if err != nil {
		logger.Error("failed to start metrics server", "error", err)
		return 1
	}
	if metricsServer != nil {
		defer func() {
			if closeErr := metricsServer.Close(); closeErr != nil {
				logger.Error("failed to close metrics server", "error", closeErr)
			}
		}()
	}

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	signalsChan := make(chan os.Signal, 1)
	signal.Notify(signalsChan, unix.SIGINT, unix.SIGTERM)

	// Start processor in goroutine
	errChan := make(chan error)
	go func() {
		errChan <- processor.Run(ctx)
	}()

	// Wait for signal or error
	exitCode := waitForShutdown(cancel, logger, errChan, signalsChan, shutdownTimeout)

	if exitCode == 0 {
		logger.Info("log-courier stopped")
	}
	return exitCode
}
