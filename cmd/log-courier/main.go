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
		ClickHouseURL:      logcourier.ConfigSpec.GetString("clickhouse.url"),
		ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
		ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
		ClickHouseTimeout:  time.Duration(logcourier.ConfigSpec.GetInt("clickhouse.timeout-seconds")) * time.Second,
		CountThreshold:     logcourier.ConfigSpec.GetInt("consumer.count-threshold"),
		TimeThresholdSec:   logcourier.ConfigSpec.GetInt("consumer.time-threshold-seconds"),
		DiscoveryInterval:  time.Duration(logcourier.ConfigSpec.GetInt("consumer.discovery-interval-seconds")) * time.Second,
		NumWorkers:         logcourier.ConfigSpec.GetInt("consumer.num-workers"),
		MaxRetries:         logcourier.ConfigSpec.GetInt("retry.max-retries"),
		InitialBackoff:     time.Duration(logcourier.ConfigSpec.GetInt("retry.initial-backoff-seconds")) * time.Second,
		MaxBackoff:         time.Duration(logcourier.ConfigSpec.GetInt("retry.max-backoff-seconds")) * time.Second,
		S3Endpoint:         logcourier.ConfigSpec.GetString("s3.endpoint"),
		S3AccessKeyID:      logcourier.ConfigSpec.GetString("s3.access-key-id"),
		S3SecretAccessKey:  logcourier.ConfigSpec.GetString("s3.secret-access-key"),
		Logger:             logger,
	}
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

	// Create processor
	ctx := context.Background()
	processorCfg := buildProcessorConfig(logger)

	processor, err := logcourier.NewProcessor(ctx, processorCfg)
	if err != nil {
		logger.Error("failed to create processor", "error", err)
		return 1
	}
	defer func() {
		if err := processor.Close(); err != nil {
			logger.Error("failed to close processor", "error", err)
		}
	}()

	// Set up signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	signalsChan := make(chan os.Signal, 1)
	signal.Notify(signalsChan, unix.SIGINT, unix.SIGTERM)

	// Start processor in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.Run(ctx)
	}()

	// Wait for signal or error
	select {
	case sig := <-signalsChan:
		logger.Info("signal received", "signal", sig)
		cancel()

		// Wait for processor to stop gracefully (with timeout)
		shutdownTimeout := time.Duration(logcourier.ConfigSpec.GetInt("shutdown-timeout-seconds")) * time.Second
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

	logger.Info("log-courier stopped")
	return 0
}
