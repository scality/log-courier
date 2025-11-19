package main

// Implementation pattern follows the Node.js/TypeScript version of scuba, utapi and backbeat:
// - https://github.com/scality/scuba/blob/main/bin/ensureServiceUser.ts
// - https://github.com/scality/utapi/blob/development/8.2/bin/ensureServiceUser2
// - https://github.com/scality/backbeat/blob/development/9.1/bin/ensureServiceUser

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/spf13/pflag"

	"github.com/scality/log-courier/pkg/ensureserviceuser"
	"github.com/scality/log-courier/pkg/util"
)

func main() {
	iamEndpoint := pflag.String("iam-endpoint", "http://localhost:8600", "Vault IAM endpoint")
	logLevel := pflag.String("log-level", "info", "Log level: debug, info, warn, error")
	pflag.Parse()

	args := pflag.Args()
	if len(args) != 2 || args[0] != "apply" {
		fmt.Fprintf(os.Stderr, "Usage: ensureServiceUser apply <service-name> [flags]\n")
		pflag.PrintDefaults()
		os.Exit(2)
	}

	serviceName := args[1]
	if serviceName == "" {
		fmt.Fprintf(os.Stderr, "Error: service-name cannot be empty\n")
		os.Exit(2)
	}

	// Logs go to stderr while JSON output goes to stdout. This separation allows
	// Ansible to parse stdout cleanly while operators can view logs on stderr for debugging.
	level := util.ParseLogLevel(*logLevel)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))

	ctx := context.Background()
	result, err := applyServiceUser(ctx, logger, serviceName, *iamEndpoint)
	if err != nil {
		// Write error JSON to stdout
		outputErr := ensureserviceuser.OutputError{
			Error: err.Error(),
		}
		if jsonErr := json.NewEncoder(os.Stdout).Encode(outputErr); jsonErr != nil {
			logger.Error("failed to encode error output", "error", jsonErr)
		}
		os.Exit(1)
	}

	output := ensureserviceuser.OutputSuccess{
		Data: *result,
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		logger.Error("failed to encode success output", "error", err)
		os.Exit(1)
	}
}

func applyServiceUser(ctx context.Context, logger *slog.Logger, serviceName, iamEndpoint string) (*ensureserviceuser.Result, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(getEnvOrDefault("AWS_REGION", "us-east-1")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID environment variable is required")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return nil, fmt.Errorf("AWS_SECRET_ACCESS_KEY environment variable is required")
	}

	iamClient := iam.NewFromConfig(cfg, func(o *iam.Options) {
		o.BaseEndpoint = aws.String(iamEndpoint)
	})

	logger.Info("applying service user configuration",
		"service", serviceName,
		"endpoint", iamEndpoint,
	)

	result, err := ensureserviceuser.Apply(ctx, iamClient, serviceName)
	if err != nil {
		logger.Error("failed to apply service user", "error", err)
		return nil, err
	}

	if result.SecretAccessKey != nil {
		logger.Info("service user created successfully with new access key")
	} else {
		logger.Info("service user already exists with existing access key")
	}

	return result, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
