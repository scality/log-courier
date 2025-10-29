package logcourier

import "fmt"

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

	timeThreshold := ConfigSpec.GetInt("consumer.time-threshold-seconds")
	if timeThreshold <= 0 {
		return fmt.Errorf("consumer.time-threshold-seconds must be positive, got %d", timeThreshold)
	}

	return nil
}
