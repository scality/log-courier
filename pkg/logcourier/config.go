package logcourier

import "fmt"

const (
	// MaxRecordsPerBatch is the hard limit to prevent OOM
	MaxRecordsPerBatch = 100_000
)

// ValidateConfig performs additional validation beyond required field checks
func ValidateConfig() error {
	logLevel := ConfigSpec.GetString("log-level")
	validLevels := map[string]bool{"error": true, "warn": true, "info": true, "debug": true}
	if !validLevels[logLevel] {
		return fmt.Errorf("invalid log-level: %s (must be error|warn|info|debug)", logLevel)
	}

	countThreshold := ConfigSpec.GetInt("consumer.count-threshold")
	if countThreshold <= 0 {
		return fmt.Errorf("consumer.count-threshold must be positive, got %d", countThreshold)
	}

	if countThreshold > MaxRecordsPerBatch {
		return fmt.Errorf("consumer.count-threshold (%d) exceeds maximum allowed (%d)", countThreshold, MaxRecordsPerBatch)
	}

	timeThreshold := ConfigSpec.GetInt("consumer.time-threshold-seconds")
	if timeThreshold <= 0 {
		return fmt.Errorf("consumer.time-threshold-seconds must be positive, got %d", timeThreshold)
	}

	maxRetryAttempts := ConfigSpec.GetInt("s3.max-retry-attempts")
	if maxRetryAttempts <= 0 {
		return fmt.Errorf("s3.max-retry-attempts must be positive, got %d", maxRetryAttempts)
	}

	maxBackoffDelay := ConfigSpec.GetInt("s3.max-backoff-delay-seconds")
	if maxBackoffDelay <= 0 {
		return fmt.Errorf("s3.max-backoff-delay-seconds must be positive, got %d", maxBackoffDelay)
	}

	return nil
}
