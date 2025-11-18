package util

import "strings"

// ParseCommaSeparatedHosts parses a comma-separated string into a slice of trimmed host strings
func ParseCommaSeparatedHosts(value string) []string {
	if value == "" {
		return []string{}
	}

	parts := strings.Split(value, ",")
	hosts := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			hosts = append(hosts, trimmed)
		}
	}

	return hosts
}
