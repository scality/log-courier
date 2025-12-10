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

	backoffJitterFactor := ConfigSpec.GetFloat64("retry.backoff-jitter-factor")
	if backoffJitterFactor < 0.0 || backoffJitterFactor > 1.0 {
		return fmt.Errorf("retry.backoff-jitter-factor must be between 0.0 and 1.0, got %f", backoffJitterFactor)
	}

	discoveryIntervalJitterFactor := ConfigSpec.GetFloat64("consumer.discovery-interval-jitter-factor")
	if discoveryIntervalJitterFactor < 0.0 || discoveryIntervalJitterFactor > 1.0 {
		return fmt.Errorf("consumer.discovery-interval-jitter-factor must be between 0.0 and 1.0, got %f", discoveryIntervalJitterFactor)
	}

	minDiscoveryInterval := ConfigSpec.GetInt("consumer.min-discovery-interval-seconds")
	if minDiscoveryInterval <= 0 {
		return fmt.Errorf("consumer.min-discovery-interval-seconds must be positive, got %d", minDiscoveryInterval)
	}

	maxDiscoveryInterval := ConfigSpec.GetInt("consumer.max-discovery-interval-seconds")
	if maxDiscoveryInterval <= 0 {
		return fmt.Errorf("consumer.max-discovery-interval-seconds must be positive, got %d", maxDiscoveryInterval)
	}

	if minDiscoveryInterval > maxDiscoveryInterval {
		return fmt.Errorf("consumer.min-discovery-interval-seconds (%d) must be <= consumer.max-discovery-interval-seconds (%d)",
			minDiscoveryInterval, maxDiscoveryInterval)
	}

	maxRetries := ConfigSpec.GetInt("retry.max-retries")
	if maxRetries < 0 {
		return fmt.Errorf("retry.max-retries must be non-negative, got %d", maxRetries)
	}

	initialBackoff := ConfigSpec.GetInt("retry.initial-backoff-seconds")
	if initialBackoff <= 0 {
		return fmt.Errorf("retry.initial-backoff-seconds must be positive, got %d", initialBackoff)
	}

	maxBackoff := ConfigSpec.GetInt("retry.max-backoff-seconds")
	if maxBackoff <= 0 {
		return fmt.Errorf("retry.max-backoff-seconds must be positive, got %d", maxBackoff)
	}

	if initialBackoff > maxBackoff {
		return fmt.Errorf("retry.initial-backoff-seconds (%d) must be <= retry.max-backoff-seconds (%d)",
			initialBackoff, maxBackoff)
	}

	return nil
}
