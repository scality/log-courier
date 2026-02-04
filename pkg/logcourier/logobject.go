package logcourier

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
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
		b.writeLogRecord(&buf, &records[i])
		buf.WriteByte('\n')
	}

	return buf.Bytes()
}

// writeLogRecord writes a single log record according to AWS format
// Takes a pointer to avoid copying the ~1KB struct on each call (called for each record in the batch)
// Field order must match AWS S3 Server Access Log format exactly
func (b *LogObjectBuilder) writeLogRecord(w *bytes.Buffer, rec *LogRecord) {
	b.writeStringPtr(w, rec.BucketOwner) // 1. Bucket Owner
	w.WriteByte(' ')
	b.writeStringPtr(w, &rec.BucketName) // 2. Bucket
	w.WriteByte(' ')
	b.writeTimestamp(w, rec.StartTime) // 3. Time
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.ClientIP) // 4. Remote IP
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.Requester) // 5. Requester
	w.WriteByte(' ')
	b.writeStringPtr(w, &rec.ReqID) // 6. Request ID
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.Operation) // 7. Operation
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.ObjectKey) // 8. Key
	w.WriteByte(' ')
	b.writeQuotedStringPtr(w, rec.RequestURI) // 9. Request-URI (quoted)
	w.WriteByte(' ')
	b.writeUint16Ptr(w, rec.HttpCode) // 10. HTTP Status
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.ErrorCode) // 11. Error Code
	w.WriteByte(' ')
	b.writeUint64Ptr(w, rec.BytesSent) // 12. Bytes Sent
	w.WriteByte(' ')
	b.writeUint64Ptr(w, rec.ObjectSize) // 13. Object Size
	w.WriteByte(' ')
	b.writeFloat32Ptr(w, rec.TotalTime) // 14. Total Time
	w.WriteByte(' ')
	b.writeFloat32Ptr(w, rec.TurnAroundTime) // 15. Turn-Around Time
	w.WriteByte(' ')
	b.writeQuotedStringPtr(w, rec.Referer) // 16. Referer (quoted)
	w.WriteByte(' ')
	b.writeQuotedStringPtr(w, rec.UserAgent) // 17. User-Agent (quoted)
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.VersionID)        // 18. Version Id
	w.WriteString(" - ")                      // 19. Host Id (not implemented)
	b.writeStringPtr(w, rec.SignatureVersion) // 20. Signature Version
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.CipherSuite) // 21. Cipher Suite
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.AuthenticationType) // 22. Authentication Type
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.HostHeader) // 23. Host Header
	w.WriteByte(' ')
	b.writeStringPtr(w, rec.TlsVersion)  // 24. TLS Version
	w.WriteString(" - ")                 // 25. Access Point ARN (not implemented)
	b.writeStringPtr(w, rec.AclRequired) // 26. ACL Required
}

// writeStringPtr writes a nullable unquoted string field to a bytes.Buffer
// Both NULL and empty string → "-" (cannot distinguish in space-delimited format)
func (b *LogObjectBuilder) writeStringPtr(w *bytes.Buffer, s *string) {
	if s == nil || *s == "" {
		w.WriteByte('-')
		return
	}
	w.WriteString(*s)
}

// writeQuotedStringPtr writes a nullable quoted string field to a bytes.Buffer
// Both NULL and empty string -> "-"
// Non-empty -> quoted value
// Fields that need quoting: Request-URI, Referer, User-Agent
func (b *LogObjectBuilder) writeQuotedStringPtr(w *bytes.Buffer, s *string) {
	if s == nil || *s == "" {
		w.WriteByte('-')
		return
	}
	w.WriteString(strconv.Quote(*s))
}

// writeTimestamp writes a timestamp in AWS format: [DD/MMM/YYYY:HH:MM:SS +0000]
// to a bytes.Buffer
// Always outputs in UTC timezone
func (b *LogObjectBuilder) writeTimestamp(w *bytes.Buffer, t time.Time) {
	utc := t.UTC()
	w.WriteString(utc.Format("[02/Jan/2006:15:04:05 +0000]"))
}

// writeUint16Ptr writes a pointer to uint16 to a bytes.Buffer, using "-" for NULL
func (b *LogObjectBuilder) writeUint16Ptr(w *bytes.Buffer, n *uint16) {
	if n == nil {
		w.WriteByte('-')
		return
	}
	fmt.Fprintf(w, "%d", *n)
}

// writeUint64Ptr writes a pointer to uint64 to a bytes.Buffer, using "-" for NULL
func (b *LogObjectBuilder) writeUint64Ptr(w *bytes.Buffer, n *uint64) {
	if n == nil {
		w.WriteByte('-')
		return
	}
	fmt.Fprintf(w, "%d", *n)
}

// writeFloat32Ptr writes a pointer to float32 to a bytes.Buffer, using "-" for NULL
func (b *LogObjectBuilder) writeFloat32Ptr(w *bytes.Buffer, f *float32) {
	if f == nil {
		w.WriteByte('-')
		return
	}
	fmt.Fprintf(w, "%g", *f)
}
