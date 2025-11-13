package logcourier

import "time"

// LogRecord represents a single log record from ClickHouse
// Maps to the AWS S3 Server Access Log format fields
type LogRecord struct {
	StartTime  time.Time `ch:"startTime"`  // AWS field 3: Time (request start time)
	InsertedAt time.Time `ch:"insertedAt"` // ClickHouse insertion timestamp (for offset tracking)
	Timestamp  time.Time `ch:"timestamp"`  // Event timestamp

	BucketOwner        string `ch:"bucketOwner"`        // AWS field 1: Bucket Owner
	BucketName         string `ch:"bucketName"`         // AWS field 2: Bucket
	ClientIP           string `ch:"clientIP"`           // AWS field 4: Remote IP
	Requester          string `ch:"requester"`          // AWS field 5: Requester
	ReqID              string `ch:"req_id"`             // AWS field 6: Request ID
	Action             string `ch:"action"`             // AWS field 7: Operation
	ObjectKey          string `ch:"objectKey"`          // AWS field 8: Key
	RequestURI         string `ch:"requestURI"`         // AWS field 9: Request-URI
	ErrorCode          string `ch:"errorCode"`          // AWS field 11: Error Code
	Referer            string `ch:"referer"`            // AWS field 16: Referer
	UserAgent          string `ch:"userAgent"`          // AWS field 17: User-Agent
	VersionID          string `ch:"versionId"`          // AWS field 18: Version Id
	SignatureVersion   string `ch:"signatureVersion"`   // AWS field 20: Signature Version
	CipherSuite        string `ch:"cipherSuite"`        // AWS field 21: Cipher Suite
	AuthenticationType string `ch:"authenticationType"` // AWS field 22: Authentication Type
	HostHeader         string `ch:"hostHeader"`         // AWS field 23: Host Header
	TlsVersion         string `ch:"tlsVersion"`         // AWS field 24: TLS Version
	AclRequired        string `ch:"aclRequired"`        // AWS field 26: ACL Required

	// Internal
	LoggingTargetBucket string `ch:"loggingTargetBucket"` // Destination bucket for log object
	LoggingTargetPrefix string `ch:"loggingTargetPrefix"` // Prefix for log object key

	BytesSent     uint64 `ch:"bytesSent"`     // AWS field 12: Bytes Sent
	ContentLength uint64 `ch:"contentLength"` // AWS field 13: Object Size

	ElapsedMs      float32 `ch:"elapsed_ms"`     // AWS field 14: Total Time
	TurnAroundTime float32 `ch:"turnAroundTime"` // AWS field 15: Turn-Around Time

	HttpCode      uint16 `ch:"httpCode"`      // AWS field 10: HTTP Status
	RaftSessionID uint16 `ch:"raftSessionID"` // Bucket raft session ID
}
