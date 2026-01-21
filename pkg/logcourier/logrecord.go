package logcourier

import "time"

// LogRecord represents a single log record from ClickHouse
// Maps to the AWS S3 Server Access Log format fields
//
//nolint:govet // fieldalignment: logical field grouping preferred over minor memory optimization
type LogRecord struct {
	// AWS S3 Server Access Log fields (ordered by AWS format)
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
	BucketOwner    *string   `ch:"bucketOwner"`    // 1. Bucket Owner
	BucketName     string    `ch:"bucketName"`     // 2. Bucket
	StartTime      time.Time `ch:"startTime"`      // 3. Time
	ClientIP       *string   `ch:"clientIP"`       // 4. Remote IP
	Requester      *string   `ch:"requester"`      // 5. Requester
	ReqID          string    `ch:"req_id"`         // 6. Request ID
	Operation      *string   `ch:"operation"`      // 7. Operation
	ObjectKey      *string   `ch:"objectKey"`      // 8. Key
	RequestURI     *string   `ch:"requestURI"`     // 9. Request-URI
	HttpCode       *uint16   `ch:"httpCode"`       // 10. HTTP status
	ErrorCode      *string   `ch:"errorCode"`      // 11. Error Code
	BytesSent      *uint64   `ch:"bytesSent"`      // 12. Bytes Sent
	ObjectSize     *uint64   `ch:"objectSize"`     // 13. Object Size
	TotalTime      *float32  `ch:"totalTime"`      // 14. Total Time
	TurnAroundTime *float32  `ch:"turnAroundTime"` // 15. Turn-Around Time
	Referer        *string   `ch:"referer"`        // 16. Referer
	UserAgent      *string   `ch:"userAgent"`      // 17. User-Agent
	VersionID      *string   `ch:"versionId"`      // 18. Version Id
	// 19. Host Id - omitted (not supported)
	SignatureVersion   *string `ch:"signatureVersion"`   // 20. Signature Version
	CipherSuite        *string `ch:"cipherSuite"`        // 21. Cipher Suite
	AuthenticationType *string `ch:"authenticationType"` // 22. Authentication Type
	HostHeader         *string `ch:"hostHeader"`         // 23. Host Header
	TlsVersion         *string `ch:"tlsVersion"`         // 24. TLS version
	// 25. Access Point ARN - omitted (not supported)
	AclRequired *string `ch:"aclRequired"` // 26. ACL Required

	// Internal fields (not part of AWS log format)
	InsertedAt          time.Time `ch:"insertedAt"`          // ClickHouse insertion timestamp
	LoggingTargetBucket string    `ch:"loggingTargetBucket"` // Destination bucket for log object
	LoggingTargetPrefix string    `ch:"loggingTargetPrefix"` // Prefix for log object key
	RaftSessionID       uint16    `ch:"raftSessionID"`       // Bucket raft session ID
}
