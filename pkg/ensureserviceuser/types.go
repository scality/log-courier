package ensureserviceuser

// Result represents the output of the ensureServiceUser operation.
type Result struct {
	SecretAccessKey *string `json:"SecretAccessKey,omitempty"`
	AccessKeyId     string  `json:"AccessKeyId"`
}

// OutputSuccess represents successful JSON output format.
type OutputSuccess struct {
	Data Result `json:"data"`
}

// OutputError represents error JSON output format.
type OutputError struct {
	Error string `json:"error"`
}
