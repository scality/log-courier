package logcourier

import "fmt"

// ValidateConfig performs additional validation beyond required field checks
func ValidateConfig() error {
	// Validate log level
	logLevel := ConfigSpec.GetString("log-level")
	validLevels := map[string]bool{"error": true, "warn": true, "info": true, "debug": true}
	if !validLevels[logLevel] {
		return fmt.Errorf("invalid log-level: %s (must be error|warn|info|debug)", logLevel)
	}

	return nil
}
