package logcourier

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
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
// Records are sorted by StartTime
// The key is generated using the first record's StartTime in the format:
// <LoggingTargetPrefix>YYYY-mm-DD-HH-MM-SS-UniqueString
func (b *LogObjectBuilder) Build(records []LogRecord) (LogObject, error) {
	if len(records) == 0 {
		return LogObject{}, fmt.Errorf("no records to build log object")
	}

	// Sort records by StartTime for chronological ordering in S3 files.
	sort.Slice(records, func(i, j int) bool {
		// Sort by StartTime, then req_id for stable ordering
		if records[i].StartTime.Equal(records[j].StartTime) {
			return records[i].ReqID < records[j].ReqID
		}
		return records[i].StartTime.Before(records[j].StartTime)
	})

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

	// Generate object key using first record's StartTime.
	// Records are ordered by StartTime, so first record has the earliest time.
	key, err := b.generateKey(firstRecord.LoggingTargetPrefix, firstRecord.StartTime)
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
	return fmt.Sprintf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s",
		b.formatStringPtr(rec.BucketOwner),        // 1. Bucket Owner
		b.formatString(rec.BucketName),        // 2. Bucket
		b.formatTimestamp(rec.StartTime),      // 3. Time
		b.formatStringPtr(rec.ClientIP),           // 4. Remote IP
		b.formatStringPtr(rec.Requester),          // 5. Requester
		b.formatString(rec.ReqID),             // 6. Request ID
		b.formatStringPtr(rec.Operation),          // 7. Operation
		b.formatStringPtr(rec.ObjectKey),          // 8. Key
		b.formatQuotedStringPtr(rec.RequestURI),   // 9. Request-URI (quoted)
		b.formatUint16Ptr(rec.HttpCode),       // 10. HTTP Status
		b.formatStringPtr(rec.ErrorCode),          // 11. Error Code
		b.formatUint64Ptr(rec.BytesSent),      // 12. Bytes Sent
		b.formatUint64Ptr(rec.ObjectSize),     // 13. Object Size
		b.formatFloat32Ptr(rec.TotalTime),     // 14. Total Time
		b.formatFloat32Ptr(rec.TurnAroundTime), // 15. Turn-Around Time
		b.formatQuotedStringPtr(rec.Referer),      // 16. Referer (quoted)
		b.formatQuotedStringPtr(rec.UserAgent),    // 17. User-Agent (quoted)
		b.formatStringPtr(rec.VersionID),          // 18. Version Id
		"-",                                   // 19. Host Id (not implemented)
		b.formatStringPtr(rec.SignatureVersion),   // 20. Signature Version
		b.formatStringPtr(rec.CipherSuite),        // 21. Cipher Suite
		b.formatStringPtr(rec.AuthenticationType), // 22. Authentication Type
		b.formatStringPtr(rec.HostHeader),         // 23. Host Header
		b.formatStringPtr(rec.TlsVersion),         // 24. TLS Version
		"-",                                   // 25. Access Point ARN (not implemented)
		b.formatStringPtr(rec.AclRequired),        // 26. ACL Required
	)
}

// formatStringPtr formats a nullable unquoted string field
// Both NULL and empty string → "-" (cannot distinguish in space-delimited format)
func (b *LogObjectBuilder) formatStringPtr(s *string) string {
	if s == nil || *s == "" {
		return "-"
	}
	return *s
}

// formatString formats a non-nullable string field, using "-" for empty values
func (b *LogObjectBuilder) formatString(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// formatQuotedStringPtr formats a nullable quoted string field
// Both NULL and empty string -> "-"
// Non-empty -> quoted value
// Fields that need quoting: Request-URI, Referer, User-Agent
func (b *LogObjectBuilder) formatQuotedStringPtr(s *string) string {
	if s == nil || *s == "" {
		return "-"
	}
	return fmt.Sprintf("%q", *s)
}

// formatTimestamp formats a timestamp in AWS format: [DD/MMM/YYYY:HH:MM:SS +0000]
// Always outputs in UTC timezone
func (b *LogObjectBuilder) formatTimestamp(t time.Time) string {
	utc := t.UTC()
	return utc.Format("[02/Jan/2006:15:04:05 +0000]")
}

// formatUint16Ptr formats a pointer to uint16, using "-" for NULL
func (b *LogObjectBuilder) formatUint16Ptr(n *uint16) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *n)
}

// formatUint64Ptr formats a pointer to uint64, using "-" for NULL
func (b *LogObjectBuilder) formatUint64Ptr(n *uint64) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *n)
}

// formatFloat32Ptr formats a pointer to float32, using "-" for NULL
func (b *LogObjectBuilder) formatFloat32Ptr(f *float32) string {
	if f == nil {
		return "-"
	}
	return fmt.Sprintf("%g", *f)
}
