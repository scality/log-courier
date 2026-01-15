package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
	Time           time.Time
	BucketOwner    string
	Bucket         string
	RemoteIP       string
	Requester      string
	RequestID      string
	Operation      string
	Key            string
	RequestURI     string
	ErrorCode      string
	Referer        string
	UserAgent      string
	VersionID      string
	BytesSent      int64
	ObjectSize     int64
	HTTPStatus     int
	TotalTime      int
	TurnAroundTime int
}

// ExpectedLog defines expected log record fields for verification
type ExpectedLog struct {
	Operation  string
	Bucket     string
	Key        string // Optional, use "" to skip check
	ErrorCode  string // Optional, use "" to skip check
	HTTPStatus int
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
		BucketOwner:    fields[0],
		Bucket:         fields[1],
		Time:           timestamp,
		RemoteIP:       fields[3],
		Requester:      fields[4],
		RequestID:      fields[5],
		Operation:      fields[6],
		Key:            fields[7],
		RequestURI:     strings.Trim(fields[8], "\""),
		HTTPStatus:     httpStatus,
		ErrorCode:      fields[10],
		BytesSent:      bytesSent,
		ObjectSize:     objectSize,
		TotalTime:      totalTime,
		TurnAroundTime: turnAroundTime,
		Referer:        strings.Trim(fields[15], "\""),
		UserAgent:      strings.Trim(fields[16], "\""),
		VersionID:      fields[17],
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
	objectKeys, err := findLogObjectsSince(ctx.S3Client, ctx.DestinationBucket,
		ctx.LogPrefix, since)
	if err != nil {
		return nil, err
	}

	var allRecords []*ParsedLogRecord
	for _, key := range objectKeys {
		content, err := downloadObject(ctx.S3Client, ctx.DestinationBucket, key)
		if err != nil {
			return nil, fmt.Errorf("failed to download %s: %w", key, err)
		}

		records := parseLogContent(content)
		allRecords = append(allRecords, records...)
	}

	return allRecords, nil
}

// waitForLogCount waits for at least expectedCount logs to appear
func waitForLogCount(ctx *E2ETestContext, expectedCount int, timeout time.Duration) []*ParsedLogRecord {
	var allLogs []*ParsedLogRecord

	Eventually(func() int {
		logs, err := fetchAllLogsSince(ctx, ctx.TestStartTime)
		if err != nil {
			return 0
		}
		allLogs = logs
		return len(logs)
	}, timeout, 2*time.Second).Should(BeNumerically(">=", expectedCount),
		"Expected at least %d logs within %v", expectedCount, timeout)

	return allLogs
}

// verifyLogRecord verifies a log record matches expected values
func verifyLogRecord(actual *ParsedLogRecord, expected ExpectedLog) {
	GinkgoHelper()

	Expect(actual.Operation).To(Equal(expected.Operation),
		"Operation mismatch")

	Expect(actual.Bucket).To(Equal(expected.Bucket),
		"Bucket mismatch")

	if expected.Key != "" {
		Expect(actual.Key).To(Equal(expected.Key),
			"Key mismatch")
	}

	Expect(actual.HTTPStatus).To(Equal(expected.HTTPStatus),
		"HTTP status mismatch")

	if expected.ErrorCode != "" {
		Expect(actual.ErrorCode).To(Equal(expected.ErrorCode),
			"Error code mismatch")
	}
}

// verifyChronologicalOrder verifies logs are in chronological order
func verifyChronologicalOrder(records []*ParsedLogRecord) {
	GinkgoHelper()

	for i := 1; i < len(records); i++ {
		Expect(records[i].Time.After(records[i-1].Time) ||
			records[i].Time.Equal(records[i-1].Time)).To(BeTrue(),
			"Logs should be in chronological order: record %d (%v) should be after or equal to record %d (%v)",
			i, records[i].Time, i-1, records[i-1].Time)
	}
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
	_, err := sharedS3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(sourceBucket),
	})
	Expect(err).NotTo(HaveOccurred(), "Failed to create source bucket")

	// Create destination bucket
	_, err = sharedS3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(destBucket),
	})
	Expect(err).NotTo(HaveOccurred(), "Failed to create destination bucket")

	// Configure bucket logging
	err = configureBucketLogging(sharedS3Client, sourceBucket, destBucket, logPrefix)
	Expect(err).NotTo(HaveOccurred(), "Failed to configure bucket logging")

	return &E2ETestContext{
		TestName:          testName,
		S3Client:          sharedS3Client,
		SourceBucket:      sourceBucket,
		DestinationBucket: destBucket,
		LogPrefix:         logPrefix,
		TestStartTime:     time.Now(),
	}
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

	// Delete all objects in destination bucket
	err := emptyBucket(ctx.S3Client, ctx.DestinationBucket)
	if err != nil {
		// Log but don't fail - bucket might not exist
		fmt.Printf("Warning: failed to delete objects in destination bucket: %v\n", err)
	}

	// Delete all objects in source bucket
	err = emptyBucket(ctx.S3Client, ctx.SourceBucket)
	if err != nil {
		fmt.Printf("Warning: failed to delete objects in source bucket: %v\n", err)
	}

	// Delete destination bucket
	_, err = ctx.S3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(ctx.DestinationBucket),
	})
	if err != nil {
		fmt.Printf("Warning: failed to delete destination bucket: %v\n", err)
	}

	// Delete source bucket
	_, err = ctx.S3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
		Bucket: aws.String(ctx.SourceBucket),
	})
	if err != nil {
		fmt.Printf("Warning: failed to delete source bucket: %v\n", err)
	}
}
