package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	opPutObject = "REST.PUT.OBJECT"
)

var _ = Describe("Log processing", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("processes large batches", func(ctx context.Context) {
		const numOperations = 1000
		testContent := []byte("batch test data")

		putObjects(testCtx, "batch-object-%04d.txt", numOperations, testContent)
		logs := waitForLogCount(testCtx, numOperations)

		expectedKeys := make(map[string]bool)
		for i := 0; i < numOperations; i++ {
			expectedKeys[fmt.Sprintf("batch-object-%04d.txt", i)] = true
		}

		verifyLogKeys(logs, testCtx.SourceBucket, "batch-object-", expectedKeys, opPutObject)
	})

	It("triggers processing by time threshold", func(ctx context.Context) {
		// Config has count-threshold: 2, time-threshold: 2s
		testKey := "time-threshold-test.txt"
		testContent := []byte("time threshold test data")
		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		testCtx.VerifyLogs(testCtx.ObjectOp(opPutObject, testKey, 200))
	})

	It("processes logs across multiple cycles", func(ctx context.Context) {
		const (
			numCycles   = 3
			opsPerCycle = 10
		)

		expectedKeys := make(map[string]bool)
		for cycle := 0; cycle < numCycles; cycle++ {
			for i := 0; i < opsPerCycle; i++ {
				key := fmt.Sprintf("cycle%d-obj-%d.txt", cycle+1, i)
				expectedKeys[key] = true
			}
		}

		// Process cycles sequentially, waiting for each to complete.
		for cycle := 0; cycle < numCycles; cycle++ {
			keyPrefix := fmt.Sprintf("cycle%d-obj", cycle+1)
			putObjects(testCtx, keyPrefix+"-%d.txt", opsPerCycle, nil)

			// Wait for this cycle's logs to appear.
			expectedSoFar := (cycle + 1) * opsPerCycle
			_ = waitForLogCount(testCtx, expectedSoFar)
		}

		allLogs, err := fetchAllLogsSince(testCtx, testCtx.TestStartTime)
		Expect(err).NotTo(HaveOccurred())

		verifyLogKeys(allLogs, testCtx.SourceBucket, "cycle", expectedKeys, opPutObject)
	})

	It("processes multiple buckets", func(ctx context.Context) {
		// Config has max-buckets-per-discovery=5,
		// create 12 valid buckets + 1 failing bucket (non-existent destination bucket)
		// to force at least 3 discovery cycles and test error handling.
		const (
			numValidBuckets = 12
			opsPerBucket    = 2
			totalExpected   = numValidBuckets * opsPerBucket
		)

		timestamp := time.Now().UnixNano()
		validBuckets := make([]string, numValidBuckets)
		validBuckets[0] = testCtx.SourceBucket

		// Create the remaining valid buckets
		for i := 1; i < numValidBuckets; i++ {
			bucketName := fmt.Sprintf("e2e-multi-bucket-%d-%d", timestamp, i)
			err := createBucketWithRetry(testCtx.S3Client, bucketName)
			Expect(err).NotTo(HaveOccurred(), "Failed to create bucket %d", i)

			err = configureBucketLogging(testCtx.S3Client, bucketName,
				testCtx.DestinationBucket, testCtx.LogPrefix)
			Expect(err).NotTo(HaveOccurred(), "Failed to configure logging for bucket %d", i)

			validBuckets[i] = bucketName
		}

		// Create a bucket with logging to a destination that will be deleted
		failingBucket := fmt.Sprintf("e2e-failing-%d", timestamp)
		doomedDest := fmt.Sprintf("e2e-doomed-dest-%d", timestamp)

		err := createBucketWithRetry(testCtx.S3Client, failingBucket)
		Expect(err).NotTo(HaveOccurred(), "Failed to create failing bucket")
		err = createBucketWithRetry(testCtx.S3Client, doomedDest)
		Expect(err).NotTo(HaveOccurred(), "Failed to create doomed destination bucket")
		err = configureBucketLogging(testCtx.S3Client, failingBucket,
			doomedDest, testCtx.LogPrefix)
		Expect(err).NotTo(HaveOccurred(), "Failed to configure logging for failing bucket")
		err = deleteBucketWithRetry(testCtx.S3Client, doomedDest)
		Expect(err).NotTo(HaveOccurred(), "Failed to delete doomed destination bucket")

		// Cleanup all additional buckets after test
		defer func() {
			for i := 1; i < numValidBuckets; i++ {
				_, _ = testCtx.S3Client.PutBucketLogging(ctx,
					&s3.PutBucketLoggingInput{
						Bucket:              aws.String(validBuckets[i]),
						BucketLoggingStatus: &types.BucketLoggingStatus{},
					})
				_ = deleteBucketWithRetry(testCtx.S3Client, validBuckets[i])
			}
			_, _ = testCtx.S3Client.PutBucketLogging(ctx,
				&s3.PutBucketLoggingInput{
					Bucket:              aws.String(failingBucket),
					BucketLoggingStatus: &types.BucketLoggingStatus{},
				})
			_ = deleteBucketWithRetry(testCtx.S3Client, failingBucket)
		}()

		// Perform operations on the failing bucket first
		for j := 0; j < opsPerBucket; j++ {
			key := fmt.Sprintf("failing-bucket-test-%d.txt", j)
			_, putErr := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(failingBucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("failing bucket object %d", j))),
			})
			Expect(putErr).NotTo(HaveOccurred(), "PUT to failing bucket should succeed")
		}

		// Perform operations on each valid bucket
		for i, bucket := range validBuckets {
			for j := 0; j < opsPerBucket; j++ {
				key := fmt.Sprintf("multi-bucket-test-%d.txt", j)
				_, putErr := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   bytes.NewReader([]byte(fmt.Sprintf("bucket %d object %d", i, j))),
				})
				Expect(putErr).NotTo(HaveOccurred(), "PUT to bucket %d should succeed", i)
			}
		}

		logs := waitForLogCount(testCtx, totalExpected)

		// Build set of expected (bucket, key) pairs
		type bucketKey struct {
			bucket string
			key    string
		}
		expectedPairs := make(map[bucketKey]bool)
		for _, bucket := range validBuckets {
			for j := 0; j < opsPerBucket; j++ {
				key := fmt.Sprintf("multi-bucket-test-%d.txt", j)
				expectedPairs[bucketKey{bucket, key}] = true
			}
		}

		// Verify each matching log has correct operation and track seen pairs
		seenPairs := make(map[bucketKey]bool)
		for _, log := range logs {
			if !strings.HasPrefix(log.Key, "multi-bucket-test-") {
				continue
			}
			pair := bucketKey{log.Bucket, log.Key}

			Expect(log.Operation).To(Equal(opPutObject),
				"Expected PUT operation for %s/%s", log.Bucket, log.Key)
			Expect(expectedPairs[pair]).To(BeTrue(),
				"Unexpected bucket/key: %s/%s", log.Bucket, log.Key)
			Expect(seenPairs[pair]).To(BeFalse(),
				"Duplicate bucket/key: %s/%s", log.Bucket, log.Key)
			seenPairs[pair] = true
		}

		Expect(seenPairs).To(HaveLen(totalExpected),
			"Expected %d unique bucket/key pairs, got %d", totalExpected, len(seenPairs))

	})

	It("delivers logs to new prefix after logging reconfiguration", func(ctx context.Context) {
		const newPrefix = "reconfigured-prefix/"
		// Extended timeout: may need multiple batch cycles for prefix change to take effect
		reconfigTimeout := 60 * time.Second

		// Reconfigure source bucket to use new prefix.
		err := configureBucketLogging(testCtx.S3Client, testCtx.SourceBucket,
			testCtx.DestinationBucket, newPrefix)
		Expect(err).NotTo(HaveOccurred(), "Failed to reconfigure bucket logging")

		// Keep doing PUT operations until logs appear under the new prefix.
		// Due to batching, initial operations may be delivered with the old prefix.
		var logsInNewPrefix []*ParsedLogRecord
		var opCount int

		Eventually(func() int {
			opCount++
			key := fmt.Sprintf("after-reconfig-%d.txt", opCount)
			_, putErr := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(testCtx.SourceBucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("data %d", opCount))),
			})
			Expect(putErr).NotTo(HaveOccurred(), "PUT operation %d should succeed", opCount)

			logs, fetchErr := fetchLogsFromPrefix(testCtx.S3Client, testCtx.DestinationBucket,
				newPrefix, testCtx.TestStartTime)
			if fetchErr != nil {
				return 0
			}
			logsInNewPrefix = logs
			return len(logs)
		}, reconfigTimeout, logPollInterval).Should(BeNumerically(">=", 1),
			"Expected logs to appear under new prefix %s within %v", newPrefix, reconfigTimeout)

		Expect(logsInNewPrefix).NotTo(BeEmpty(), "Should have at least one log under new prefix")
	})
})
