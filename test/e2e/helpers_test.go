package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// logWaitTimeout is the maximum time to wait for logs to appear
	logWaitTimeout = 20 * time.Second
	// logPollInterval is how often to check for new logs
	logPollInterval = 2 * time.Second
	// bucketOperationMaxRetries is the number of retries after the initial attempt for bucket operations
	bucketOperationMaxRetries = 4
	// bucketOperationInitialDelay is the initial delay for exponential backoff
	bucketOperationInitialDelay = 100 * time.Millisecond
	// bucketOperationMaxDelay is the maximum delay between retries
	bucketOperationMaxDelay = 2 * time.Second
)

// E2ETestContext holds the test context for an E2E test
type E2ETestContext struct {
	TestName          string
	TestStartTime     time.Time
	S3Client          *s3.Client
	SourceBucket      string
	DestinationBucket string
	LogPrefix         string
}

// ParsedLogRecord represents a parsed S3 Server Access Log entry
type ParsedLogRecord struct {
	Time               time.Time
	BucketOwner        string
	Bucket             string
	RemoteIP           string
	Requester          string
	RequestID          string
	Operation          string
	Key                string
	RequestURI         string
	ErrorCode          string
	Referer            string
	UserAgent          string
	VersionID          string
	HostID             string // Field 19 - always "-" (not supported)
	SignatureVersion   string
	CipherSuite        string
	AuthenticationType string
	HostHeader         string
	TLSVersion         string
	AccessPointARN     string // Field 25 - always "-" (not supported)
	ACLRequired        string // Field 26 - always "-" (not supported)
	BytesSent          int64
	ObjectSize         int64
	HTTPStatus         int
	TotalTime          int
	TurnAroundTime     int
}

// ExpectedLog defines expected log record fields for verification
type ExpectedLog struct {
	Operation  string
	Bucket     string
	Key        string // Optional, use "" to skip check
	ErrorCode  string // Optional, use "" to skip check
	HTTPStatus int
	BytesSent  int64 // Optional, use -1 to skip check
	ObjectSize int64 // Optional, use -1 to skip check
}

// ExpectedLogBuilder allows fluent construction of ExpectedLog
type ExpectedLogBuilder struct {
	log ExpectedLog
}

// WithBytesSent sets the expected BytesSent value
func (b ExpectedLogBuilder) WithBytesSent(n int64) ExpectedLogBuilder {
	b.log.BytesSent = n
	return b
}

// WithObjectSize sets the expected ObjectSize value
func (b ExpectedLogBuilder) WithObjectSize(size int64) ExpectedLogBuilder {
	b.log.ObjectSize = size
	return b
}

// WithErrorCode sets the expected ErrorCode value
func (b ExpectedLogBuilder) WithErrorCode(code string) ExpectedLogBuilder {
	b.log.ErrorCode = code
	return b
}

// BucketOp creates an ExpectedLog for bucket-level operations (no key)
func (ctx *E2ETestContext) BucketOp(operation string, httpStatus int) ExpectedLogBuilder {
	return ExpectedLogBuilder{
		log: ExpectedLog{
			Operation:  operation,
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: httpStatus,
			BytesSent:  -1,
			ObjectSize: -1,
		},
	}
}

// ObjectOp creates an ExpectedLog for object-level operations
func (ctx *E2ETestContext) ObjectOp(operation, key string, httpStatus int) ExpectedLogBuilder {
	return ExpectedLogBuilder{
		log: ExpectedLog{
			Operation:  operation,
			Bucket:     ctx.SourceBucket,
			Key:        key,
			HTTPStatus: httpStatus,
			BytesSent:  -1,
			ObjectSize: -1,
		},
	}
}

// emptyBucket deletes all objects in a bucket
func emptyBucket(client *s3.Client, bucket string) error {
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return err
		}

		if len(page.Contents) == 0 {
			continue
		}

		var objectIds []types.ObjectIdentifier
		for _, obj := range page.Contents {
			objectIds = append(objectIds, types.ObjectIdentifier{
				Key: obj.Key,
			})
		}

		_, err = client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: objectIds,
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// configureBucketLogging configures S3 bucket logging
func configureBucketLogging(client *s3.Client, sourceBucket, targetBucket, prefix string) error {
	_, err := client.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{
		Bucket: aws.String(sourceBucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{
			LoggingEnabled: &types.LoggingEnabled{
				TargetBucket: aws.String(targetBucket),
				TargetPrefix: aws.String(prefix),
			},
		},
	})
	return err
}

// configureBucketPolicyForCrossAccountAccess sets up a bucket policy granting
// PutObject permission to the service-access-logging-user. Required for cross-account
// access in Integration environments.
func configureBucketPolicyForCrossAccountAccess(client *s3.Client, bucket string) error {
	policy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [
			{
				"Sid": "AllowCrossAccountPutObject",
				"Effect": "Allow",
				"Principal": {
					"AWS": "arn:aws:iam::000000000000:user/scality-internal/service-access-logging-user"
				},
				"Action": "s3:PutObject",
				"Resource": "arn:aws:s3:::%s/*"
			}
		]
	}`, bucket)

	_, err := client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(policy),
	})
	return err
}

// findLogObjectsSince finds all log objects in a bucket created after a given time
// returns the list of object keys that were created after the given time
func findLogObjectsSince(client *s3.Client, bucket, prefix string, since time.Time) ([]string, error) {
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			if obj.LastModified.After(since) || obj.LastModified.Equal(since) {
				keys = append(keys, *obj.Key)
			}
		}
	}

	return keys, nil
}

// downloadObject downloads an object from S3
// returns the object content as a byte slice
func downloadObject(client *s3.Client, bucket, key string) ([]byte, error) {
	result, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = result.Body.Close() }()

	return io.ReadAll(result.Body)
}

// parseLogFields parses space-delimited fields with quoted strings and bracketed fields
// returns a slice of strings, each representing a field
func parseLogFields(line string) []string {
	var fields []string
	var current strings.Builder
	inQuotes := false
	inBrackets := false

	for _, ch := range line {
		switch ch {
		case '"':
			inQuotes = !inQuotes
			current.WriteRune(ch)
		case '[':
			inBrackets = true
			current.WriteRune(ch)
		case ']':
			inBrackets = false
			current.WriteRune(ch)
		case ' ':
			if inQuotes || inBrackets {
				current.WriteRune(ch)
			} else if current.Len() > 0 {
				fields = append(fields, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		fields = append(fields, current.String())
	}

	return fields
}

// parseLogLine parses a single S3 Server Access Log line
func parseLogLine(line string) (*ParsedLogRecord, error) {
	fields := parseLogFields(line)

	// AWS S3 Server Access Log format has 26 fields
	if len(fields) < 26 {
		return nil, fmt.Errorf("invalid log line: insufficient fields (got %d, need 26)", len(fields))
	}

	// Parse timestamp: [07/Jan/2026:22:18:30 +0000]
	timeStr := strings.Trim(fields[2], "[]")
	timestamp, err := time.Parse("02/Jan/2006:15:04:05 -0700", timeStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	httpStatus, _ := strconv.Atoi(fields[9])
	bytesSent, _ := strconv.ParseInt(fields[11], 10, 64)
	objectSize, _ := strconv.ParseInt(fields[12], 10, 64)
	totalTime, _ := strconv.Atoi(fields[13])
	turnAroundTime, _ := strconv.Atoi(fields[14])

	return &ParsedLogRecord{
		BucketOwner:        fields[0],
		Bucket:             fields[1],
		Time:               timestamp,
		RemoteIP:           fields[3],
		Requester:          fields[4],
		RequestID:          fields[5],
		Operation:          fields[6],
		Key:                fields[7],
		RequestURI:         strings.Trim(fields[8], "\""),
		HTTPStatus:         httpStatus,
		ErrorCode:          fields[10],
		BytesSent:          bytesSent,
		ObjectSize:         objectSize,
		TotalTime:          totalTime,
		TurnAroundTime:     turnAroundTime,
		Referer:            strings.Trim(fields[15], "\""),
		UserAgent:          strings.Trim(fields[16], "\""),
		VersionID:          fields[17],
		HostID:             fields[18],
		SignatureVersion:   fields[19],
		CipherSuite:        fields[20],
		AuthenticationType: fields[21],
		HostHeader:         fields[22],
		TLSVersion:         fields[23],
		AccessPointARN:     fields[24],
		ACLRequired:        fields[25],
	}, nil
}

// parseLogContent parses log content into ParsedLogRecord structs
func parseLogContent(content []byte) []*ParsedLogRecord {
	var records []*ParsedLogRecord

	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		record, err := parseLogLine(line)
		if err != nil {
			fmt.Printf("Failed to parse log line: %v\n", err)
			continue
		}

		records = append(records, record)
	}

	return records
}

// fetchAllLogsSince fetches and parses all log records since a given time
func fetchAllLogsSince(ctx *E2ETestContext, since time.Time) ([]*ParsedLogRecord, error) {
	return fetchLogsFromPrefix(ctx.S3Client, ctx.DestinationBucket, ctx.LogPrefix, since)
}

// fetchAllLogsInBucketSince fetches and parses all log records from the entire bucket since a given time.
// This is useful when the logging prefix may have changed during the test.
func fetchAllLogsInBucketSince(ctx *E2ETestContext, since time.Time) ([]*ParsedLogRecord, error) {
	return fetchLogsFromPrefix(ctx.S3Client, ctx.DestinationBucket, "", since)
}

// fetchLogsFromPrefix fetches and parses all log records from a specific prefix since a given time.
// Verifies that records within each log object are in chronological order.
// Returns all records merged across objects in chronological order.
// Within each object, records are verified to be in chronological order.
// Records across objects are sorted by timestamp
func fetchLogsFromPrefix(client *s3.Client, bucket, prefix string, since time.Time) ([]*ParsedLogRecord, error) {
	objectKeys, err := findLogObjectsSince(client, bucket, prefix, since)
	if err != nil {
		return nil, err
	}

	var allRecords []*ParsedLogRecord
	for _, key := range objectKeys {
		content, err := downloadObject(client, bucket, key)
		if err != nil {
			return nil, fmt.Errorf("failed to download %s: %w", key, err)
		}

		records := parseLogContent(content)

		// Verify chronological order within this object
		for i := 1; i < len(records); i++ {
			if records[i].Time.Before(records[i-1].Time) {
				return nil, fmt.Errorf("object %s: logs not in chronological order: record %d (%v) is before record %d (%v)",
					key, i, records[i].Time, i-1, records[i-1].Time)
			}
		}

		allRecords = append(allRecords, records...)
	}

	// Sort by timestamp to merge records across objects chronologically.
	// This preserves within-object ordering.
	sort.SliceStable(allRecords, func(i, j int) bool {
		return allRecords[i].Time.Before(allRecords[j].Time)
	})

	return allRecords, nil
}

// waitForLogCountWithPrefix waits for at least expectedCount logs to appear under a specific prefix.
func waitForLogCountWithPrefix(ctx *E2ETestContext, prefix string, expectedCount int) []*ParsedLogRecord {
	GinkgoHelper()

	var allLogs []*ParsedLogRecord

	Eventually(func() int {
		logs, err := fetchLogsFromPrefix(ctx.S3Client, ctx.DestinationBucket, prefix, ctx.TestStartTime)
		if err != nil {
			return 0
		}
		allLogs = logs
		return len(logs)
	}, logWaitTimeout, logPollInterval).Should(BeNumerically(">=", expectedCount),
		"Expected at least %d logs with prefix %s within %v", expectedCount, prefix, logWaitTimeout)

	return allLogs
}

// waitForLogCount waits for at least expectedCount logs to appear.
func waitForLogCount(ctx *E2ETestContext, expectedCount int) []*ParsedLogRecord {
	GinkgoHelper()
	return waitForLogCountWithPrefix(ctx, ctx.LogPrefix, expectedCount)
}

// VerifyLogs waits for logs and verifies they match expected values.
// Returns the logs for additional assertions if needed.
func (ctx *E2ETestContext) VerifyLogs(expected ...ExpectedLogBuilder) []*ParsedLogRecord {
	GinkgoHelper()

	logs := waitForLogCount(ctx, len(expected))

	for i, exp := range expected {
		verifyLogRecord(logs[i], exp)
	}

	return logs
}

// verifyLogRecord verifies a log record matches expected values
func verifyLogRecord(actual *ParsedLogRecord, expected ExpectedLogBuilder) {
	GinkgoHelper()
	exp := expected.log

	Expect(actual.Operation).To(Equal(exp.Operation),
		"Operation mismatch")

	Expect(actual.Bucket).To(Equal(exp.Bucket),
		"Bucket mismatch")

	if exp.Key != "" {
		Expect(actual.Key).To(Equal(exp.Key),
			"Key mismatch")
	}

	Expect(actual.HTTPStatus).To(Equal(exp.HTTPStatus),
		"HTTP status mismatch")

	if exp.ErrorCode != "" {
		Expect(actual.ErrorCode).To(Equal(exp.ErrorCode),
			"Error code mismatch")
	}

	// Verify common metadata fields have actual values (not "-")
	// Some operations have "-" for most fields:
	// - Copy source operations (REST.COPY.OBJECT_GET, REST.COPY.PART_GET)
	// - Batch delete operations (BATCH.DELETE.OBJECT)
	skipMetadataCheck := actual.Operation == "REST.COPY.OBJECT_GET" ||
		actual.Operation == "REST.COPY.PART_GET" ||
		actual.Operation == "BATCH.DELETE.OBJECT"
	if !skipMetadataCheck {
		Expect(actual.BucketOwner).NotTo(Equal("-"),
			"BucketOwner should be present")
		Expect(actual.RemoteIP).NotTo(Equal("-"),
			"RemoteIP should be present")
		Expect(actual.Requester).NotTo(Equal("-"),
			"Requester should be present")
		Expect(actual.RequestURI).NotTo(Equal("-"),
			"RequestURI should be present")
		Expect(actual.UserAgent).NotTo(Equal("-"),
			"UserAgent should be present")
		Expect(actual.AuthenticationType).NotTo(Equal("-"),
			"AuthenticationType should be present")
		Expect(actual.HostHeader).NotTo(Equal("-"),
			"HostHeader should be present")
		Expect(actual.SignatureVersion).NotTo(Equal("-"),
			"SignatureVersion should be present")
	}

	// Verify unsupported fields are always "-"
	Expect(actual.HostID).To(Equal("-"),
		"HostID should always be '-' (not supported)")
	Expect(actual.AccessPointARN).To(Equal("-"),
		"AccessPointARN should always be '-' (not supported)")
	Expect(actual.ACLRequired).To(Equal("-"),
		"ACLRequired should always be '-' (not supported)")

	// Verify timing fields relationship
	Expect(actual.TotalTime).To(BeNumerically(">=", 0),
		"TotalTime should be non-negative")
	Expect(actual.TurnAroundTime).To(BeNumerically(">=", 0),
		"TurnAroundTime should be non-negative")
	Expect(actual.TurnAroundTime).To(BeNumerically("<=", actual.TotalTime),
		"TurnAroundTime should not exceed TotalTime")

	// Verify optional size fields when specified (use -1 to skip)
	if exp.BytesSent >= 0 {
		Expect(actual.BytesSent).To(Equal(exp.BytesSent),
			"BytesSent mismatch")
	}
	if exp.ObjectSize >= 0 {
		Expect(actual.ObjectSize).To(Equal(exp.ObjectSize),
			"ObjectSize mismatch")
	}
}

// verifyLogKeys verifies that logs contain exactly the expected keys with no duplicates.
// It filters logs by bucket and keyPrefix, then checks:
// - Each matching log has the expected operation
// - Each key is in the expectedKeys set
// - No duplicate keys exist
// - The total count matches expected
func verifyLogKeys(logs []*ParsedLogRecord, bucket, keyPrefix string, expectedKeys map[string]bool, expectedOp string) {
	GinkgoHelper()

	seenKeys := make(map[string]bool)
	for _, log := range logs {
		if log.Bucket != bucket || !strings.HasPrefix(log.Key, keyPrefix) {
			continue
		}
		Expect(log.Operation).To(Equal(expectedOp),
			"Expected %s operation for key %s", expectedOp, log.Key)
		Expect(expectedKeys[log.Key]).To(BeTrue(),
			"Unexpected key: %s", log.Key)
		Expect(seenKeys[log.Key]).To(BeFalse(),
			"Duplicate key: %s", log.Key)
		seenKeys[log.Key] = true
	}

	Expect(seenKeys).To(HaveLen(len(expectedKeys)),
		"Expected %d unique keys, got %d", len(expectedKeys), len(seenKeys))
}

// retryWithBackoff executes a function with exponential backoff retry logic.
// The shouldRetry function determines if an error should trigger a retry (if nil, all errors are retried).
func retryWithBackoff(operation func() error, shouldRetry func(error) bool) error {
	delay := bucketOperationInitialDelay
	var lastErr error

	for attempt := 0; attempt <= bucketOperationMaxRetries; attempt++ {
		err := operation()
		if err == nil {
			return nil
		}

		// Check if we should retry this error
		if shouldRetry != nil && !shouldRetry(err) {
			return err
		}

		lastErr = err

		// Sleep before next retry (except on last attempt)
		if attempt < bucketOperationMaxRetries {
			time.Sleep(delay)
			delay *= 2
			if delay > bucketOperationMaxDelay {
				delay = bucketOperationMaxDelay
			}
		}
	}

	return lastErr
}

// createBucketWithRetry attempts to create a bucket with exponential backoff retry logic
func createBucketWithRetry(client *s3.Client, bucket string) error {
	err := retryWithBackoff(func() error {
		_, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
		})
		return err
	}, nil) // nil means retry all errors

	if err != nil {
		return fmt.Errorf("failed to create bucket %s after %d attempts: %w", bucket, bucketOperationMaxRetries+1, err)
	}
	return nil
}

// putObjects creates multiple objects with keys based on keyFormat.
// keyFormat should contain a single %d verb (e.g., "prefix-%d.txt").
// If content is nil, generates unique content per object.
func putObjects(ctx *E2ETestContext, keyFormat string, count int, content []byte) {
	for i := 0; i < count; i++ {
		key := fmt.Sprintf(keyFormat, i)
		body := content
		if body == nil {
			body = []byte(fmt.Sprintf("data %d", i))
		}
		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(body),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation %d should succeed", i)
	}
}

// newS3ClientWithCredentials creates an S3 client with the given credentials,
// using the same endpoint configuration as the shared test client.
func newS3ClientWithCredentials(accessKeyID, secretAccessKey string) *s3.Client {
	endpoint := os.Getenv("E2E_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = testS3Endpoint
	}

	return s3.NewFromConfig(aws.Config{
		Region: testRegion,
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			}, nil
		}),
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

// setupE2ETest creates and initializes an E2E test context
func setupE2ETest() *E2ETestContext {
	GinkgoHelper()

	testName := CurrentSpecReport().FullText()
	timestamp := time.Now().UnixNano()
	sourceBucket := fmt.Sprintf("e2e-source-%d", timestamp)
	destBucket := fmt.Sprintf("e2e-dest-%d", timestamp)
	logPrefix := "logs/"

	// Create source bucket
	err := createBucketWithRetry(sharedS3Client, sourceBucket)
	Expect(err).NotTo(HaveOccurred(), "Failed to create source bucket")

	// Create destination bucket
	err = createBucketWithRetry(sharedS3Client, destBucket)
	Expect(err).NotTo(HaveOccurred(), "Failed to create destination bucket")

	// Configure bucket logging
	err = configureBucketLogging(sharedS3Client, sourceBucket, destBucket, logPrefix)
	Expect(err).NotTo(HaveOccurred(), "Failed to configure bucket logging")

	// Configure bucket policy for cross-account access
	err = configureBucketPolicyForCrossAccountAccess(sharedS3Client, destBucket)
	Expect(err).NotTo(HaveOccurred(), "Failed to configure bucket policy")

	return &E2ETestContext{
		TestName:          testName,
		S3Client:          sharedS3Client,
		SourceBucket:      sourceBucket,
		DestinationBucket: destBucket,
		LogPrefix:         logPrefix,
		TestStartTime:     time.Now(),
	}
}

// deleteBucketWithRetry attempts to empty and delete a bucket with exponential backoff retry logic
func deleteBucketWithRetry(client *s3.Client, bucket string) error {
	var attemptNum int
	err := retryWithBackoff(func() error {
		attemptNum++
		// Empty bucket first - fail fast if this fails
		if err := emptyBucket(client, bucket); err != nil {
			return fmt.Errorf("failed to empty bucket on attempt %d: %w", attemptNum, err)
		}

		// Then delete bucket
		_, err := client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
		return err
	}, func(err error) bool {
		// Don't retry emptyBucket errors
		if strings.Contains(err.Error(), "failed to empty bucket") {
			return false
		}
		// Only retry BucketNotEmpty errors
		return strings.Contains(err.Error(), "BucketNotEmpty")
	})

	if err != nil {
		// If it's already wrapped with attempt info or is an emptyBucket error, return as-is
		if strings.Contains(err.Error(), "failed to empty bucket") {
			return err
		}
		return fmt.Errorf("failed to delete bucket %s after %d attempts: %w", bucket, bucketOperationMaxRetries+1, err)
	}
	return nil
}

// cleanupE2ETest cleans up resources created by setupE2ETest
func cleanupE2ETest(ctx *E2ETestContext) {
	GinkgoHelper()

	if ctx == nil {
		return
	}

	// Log test context on failure for debugging
	if CurrentSpecReport().Failed() {
		fmt.Printf("[E2E Cleanup] FAILED - Test: %s\n", ctx.TestName)
		fmt.Printf("[E2E Cleanup] FAILED - Source bucket: %s\n", ctx.SourceBucket)
		fmt.Printf("[E2E Cleanup] FAILED - Destination bucket: %s\n", ctx.DestinationBucket)
	}

	// Disable logging on source bucket before cleanup.
	// Use a time slightly before the disable call to avoid flakiness.
	timeBeforeDisable := time.Now().Add(-2 * time.Second)
	_, err := ctx.S3Client.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{
		Bucket:              aws.String(ctx.SourceBucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{},
	})
	if err != nil {
		fmt.Printf("Warning: failed to disable logging on source bucket: %v\n", err)
	} else {
		// Wait for the disable logging operation to be logged.
		// This ensures logs are delivered before we delete the destination bucket,
		// preventing "NoSuchBucket" errors in log-courier.
		// Search the entire bucket since tests may change the logging prefix.
		Eventually(func() bool {
			logs, fetchErr := fetchAllLogsInBucketSince(ctx, ctx.TestStartTime)
			if fetchErr != nil {
				return false
			}

			for _, log := range logs {
				if log.Operation == "REST.PUT.LOGGING_STATUS" &&
					log.Bucket == ctx.SourceBucket &&
					log.Time.After(timeBeforeDisable) {
					return true
				}
			}
			return false
		}).WithTimeout(30 * time.Second).
			WithPolling(2 * time.Second).
			Should(BeTrue())
	}

	// Delete source bucket
	err = deleteBucketWithRetry(ctx.S3Client, ctx.SourceBucket)
	if err != nil {
		fmt.Printf("Warning: failed to delete source bucket: %v\n", err)
	}

	// Delete destination bucket
	err = deleteBucketWithRetry(ctx.S3Client, ctx.DestinationBucket)
	if err != nil {
		fmt.Printf("Warning: failed to delete destination bucket: %v\n", err)
	}
}

func verifyPerObjectSSE(ctx context.Context, testCtx *E2ETestContext, algorithm types.ServerSideEncryption) {
	GinkgoHelper()

	putKey := "sse-put-object.txt"
	putContent := []byte("test data with per-object SSE")
	_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(testCtx.SourceBucket),
		Key:                  aws.String(putKey),
		Body:                 bytes.NewReader(putContent),
		ServerSideEncryption: algorithm,
	})
	Expect(err).NotTo(HaveOccurred(), "PUT with %s should succeed", algorithm)

	copyKey := "sse-copy-object.txt"
	_, err = testCtx.S3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:               aws.String(testCtx.SourceBucket),
		Key:                  aws.String(copyKey),
		CopySource:           aws.String(fmt.Sprintf("%s/%s", testCtx.SourceBucket, putKey)),
		ServerSideEncryption: algorithm,
	})
	Expect(err).NotTo(HaveOccurred(), "COPY with %s should succeed", algorithm)

	mputKey := "sse-multipart-object.txt"
	partData := bytes.Repeat([]byte("c"), 5*1024*1024) // 5MB minimum part size
	createResp, err := testCtx.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               aws.String(testCtx.SourceBucket),
		Key:                  aws.String(mputKey),
		ServerSideEncryption: algorithm,
	})
	Expect(err).NotTo(HaveOccurred(), "CreateMultipartUpload with %s should succeed", algorithm)

	partResp, err := testCtx.S3Client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(testCtx.SourceBucket),
		Key:        aws.String(mputKey),
		PartNumber: aws.Int32(1),
		UploadId:   createResp.UploadId,
		Body:       bytes.NewReader(partData),
	})
	Expect(err).NotTo(HaveOccurred(), "UploadPart should succeed")

	_, err = testCtx.S3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(testCtx.SourceBucket),
		Key:      aws.String(mputKey),
		UploadId: createResp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{
				{
					ETag:       partResp.ETag,
					PartNumber: aws.Int32(1),
				},
			},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "CompleteMultipartUpload should succeed")

	testCtx.VerifyLogs(
		testCtx.ObjectOp(opPutObject, putKey, 200).WithObjectSize(int64(len(putContent))),
		testCtx.ObjectOp("REST.COPY.OBJECT_GET", putKey, 200).WithObjectSize(int64(len(putContent))),
		testCtx.ObjectOp("REST.COPY.OBJECT", copyKey, 200).WithObjectSize(int64(len(putContent))),
		testCtx.ObjectOp("REST.POST.UPLOADS", mputKey, 200),
		testCtx.ObjectOp("REST.PUT.PART", mputKey, 200),
		testCtx.ObjectOp("REST.POST.UPLOAD", mputKey, 200).WithObjectSize(int64(len(partData))),
	)
}

func verifyLogDeliveryWithEncryption(ctx context.Context, testCtx *E2ETestContext, bucket string, algorithm types.ServerSideEncryption) {
	GinkgoHelper()

	testKey := "sse-test-object.txt"
	testContent := []byte("test data for SSE bucket")

	_, err := testCtx.S3Client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(bucket),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: algorithm,
					},
				},
			},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "PutBucketEncryption with %s should succeed", algorithm)

	_, err = testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testCtx.SourceBucket),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(testContent),
	})
	Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

	testCtx.VerifyLogs(
		testCtx.ObjectOp(opPutObject, testKey, 200).WithObjectSize(int64(len(testContent))),
	)
}
