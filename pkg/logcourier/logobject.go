package logcourier

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// LogObject represents a log object to be written to S3
type LogObject struct {
	Key     string
	Content []byte
}

// LogObjectBuilder builds log objects from log records in AWS S3 Server Access Log format
type LogObjectBuilder struct{}

// NewLogObjectBuilder creates a new log object builder
func NewLogObjectBuilder() *LogObjectBuilder {
	return &LogObjectBuilder{}
}

// Build builds a log object from log records
// Records must be ordered by event timestamp (log record date).
// The key is generated using the first record's event timestamp in the format:
// <LoggingTargetPrefix>YYYY-mm-DD-HH-MM-SS-UniqueString
func (b *LogObjectBuilder) Build(records []LogRecord) (LogObject, error) {
	if len(records) == 0 {
		return LogObject{}, fmt.Errorf("no records to build log object")
	}

	// Configuration Handling: Use logging configuration from the first record.
	//
	// If logging configuration (loggingTargetPrefix, loggingTargetBucket) changes mid-batch,
	// records with the new configuration will be written using the old configuration
	// until the next batch is processed.
	//
	// This is an acceptable trade-off because:
	// 1. Configuration changes are expected to be infrequent
	// 2. The propagation delay (one batch cycle) is acceptable for config changes
	//
	// Alternative: BatchFinder could GROUP BY logging config to ensure homogeneous batches,
	// but this adds query complexity for minimal benefit.
	firstRecord := records[0]

	// Generate object key using first record's event timestamp.
	// Records are ordered by event timestamp (requirement: "Log records within a log object
	// must be ordered by log record date"), so first record has the earliest event time.
	key, err := b.generateKey(firstRecord.LoggingTargetPrefix, firstRecord.Timestamp)
	if err != nil {
		return LogObject{}, fmt.Errorf("failed to generate key: %w", err)
	}

	// Format log records according to AWS S3 Server Access Log format
	content := b.formatLogRecords(records)

	return LogObject{
		Key:     key,
		Content: content,
	}, nil
}

// generateKey generates a log object key using AWS format:
// <TargetPrefix>YYYY-mm-DD-HH-MM-SS-UniqueString
func (b *LogObjectBuilder) generateKey(prefix string, timestamp time.Time) (string, error) {
	// Generate random 16-character hex string for uniqueness
	randomBytes := make([]byte, 8)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("failed to generate random suffix: %w", err)
	}
	uniqueString := strings.ToUpper(hex.EncodeToString(randomBytes))

	// Format timestamp as YYYY-mm-DD-HH-MM-SS
	timeStr := timestamp.UTC().Format("2006-01-02-15-04-05")

	// Combine prefix, timestamp, and unique string
	key := fmt.Sprintf("%s%s-%s", prefix, timeStr, uniqueString)

	return key, nil
}

// formatLogRecords formats log records according to AWS S3 Server Access Log format
// Format is space-delimited with specific quoting rules
func (b *LogObjectBuilder) formatLogRecords(records []LogRecord) []byte {
	var buf bytes.Buffer

	for i := range records {
		line := b.formatLogRecord(&records[i])
		buf.WriteString(line)
		buf.WriteString("\n")
	}

	return buf.Bytes()
}

// formatLogRecord formats a single log record according to AWS format
// Takes a pointer to avoid copying the ~1KB struct on each call (called for each record in the batch)
// Field order must match AWS S3 Server Access Log format exactly
//
// Note: ClickHouse stores "-" for fields not applicable to an operation
func (b *LogObjectBuilder) formatLogRecord(rec *LogRecord) string {
	return fmt.Sprintf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
		b.formatField(rec.BucketOwner),        // 1. Bucket Owner
		b.formatField(rec.BucketName),         // 2. Bucket
		b.formatTimestamp(rec.StartTime),      // 3. Time
		b.formatField(rec.ClientIP),           // 4. Remote IP
		b.formatField(rec.Requester),          // 5. Requester
		b.formatField(rec.ReqID),              // 6. Request ID
		b.formatField(rec.Action),             // 7. Operation
		b.formatField(rec.ObjectKey),          // 8. Key
		b.formatQuotedField(rec.HttpURL),      // 9. Request-URI (quoted)
		b.formatUint16(rec.HttpCode),          // 10. HTTP Status
		b.formatField(rec.ErrorCode),          // 11. Error Code
		b.formatUint64(rec.BytesSent),         // 12. Bytes Sent
		b.formatUint64(rec.ContentLength),     // 13. Object Size
		b.formatFloat32(rec.ElapsedMs),        // 14. Total Time
		b.formatFloat32(rec.TurnAroundTime),   // 15. Turn-Around Time
		b.formatQuotedField(rec.Referer),      // 16. Referer (quoted)
		b.formatQuotedField(rec.UserAgent),    // 17. User-Agent (quoted)
		b.formatField(rec.VersionID),          // 18. Version Id
		// 19. Host Id - OMITTED
		b.formatField(rec.SignatureVersion),   // 20. Signature Version
		b.formatField(rec.CipherSuite),        // 21. Cipher Suite
		b.formatField(rec.AuthenticationType), // 22. Authentication Type
		b.formatField(rec.HostHeader),         // 23. Host Header
		b.formatField(rec.TlsVersion),         // 24. TLS Version
		// 25. Access Point ARN - OMITTED
		b.formatField(rec.AclRequired),        // 26. ACL Required
	)
}

// formatField formats a string field, using "-" for empty values
func (b *LogObjectBuilder) formatField(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// formatQuotedField formats a quoted string field, using "-" for empty values
// Fields that need quoting: Request-URI, Referer, User-Agent
func (b *LogObjectBuilder) formatQuotedField(s string) string {
	if s == "" {
		return "-"
	}
	// Quote the field
	return fmt.Sprintf("%q", s)
}

// formatTimestamp formats a timestamp in AWS format: [DD/MMM/YYYY:HH:MM:SS +0000]
// Always outputs in UTC timezone
func (b *LogObjectBuilder) formatTimestamp(t time.Time) string {
	utc := t.UTC()

	return utc.Format("[02/Jan/2006:15:04:05 +0000]")
}

// formatUint16 formats a uint16 as a decimal string
func (b *LogObjectBuilder) formatUint16(n uint16) string {
	return fmt.Sprintf("%d", n)
}

// formatUint64 formats a uint64 as a decimal string
func (b *LogObjectBuilder) formatUint64(n uint64) string {
	return fmt.Sprintf("%d", n)
}

// formatFloat32 formats a float32 as a string
func (b *LogObjectBuilder) formatFloat32(f float32) string {
	return fmt.Sprintf("%g", f)
}
