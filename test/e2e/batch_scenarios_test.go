package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Batch Scenarios", func() {
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("processes large batches (1000+ operations)", func() {
		const numOperations = 1000
		testContent := []byte("batch test data")

		By(fmt.Sprintf("performing %d PUT operations", numOperations))
		for i := 0; i < numOperations; i++ {
			testKey := fmt.Sprintf("batch-object-%04d.txt", i)
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
				Body:   bytes.NewReader(testContent),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT operation %d should succeed", i)
		}

		// Wait for all logs to appear (with generous timeout for large batch)
		By(fmt.Sprintf("waiting for %d logs to appear in destination bucket", numOperations))
		logs := waitForLogCountWithTimeout(ctx, numOperations, 2*time.Minute)

		// Verify we got all operations logged
		By("verifying all operations were logged")
		Expect(len(logs)).To(BeNumerically(">=", numOperations),
			"Should have logged at least %d operations", numOperations)

		// Analyze batch test operations for duplicates
		By("analyzing batch test PUT operations for duplicates")
		keyCount := make(map[string]int)
		requestIDCount := make(map[string]int)
		requestIDToTimes := make(map[string][]time.Time)
		batchPutCount := 0

		for _, log := range logs {
			if log.Operation == "REST.PUT.OBJECT" &&
				log.Bucket == ctx.SourceBucket &&
				len(log.Key) >= 13 &&
				log.Key[:13] == "batch-object-" {
				batchPutCount++
				keyCount[log.Key]++
				requestIDCount[log.RequestID]++
				requestIDToTimes[log.RequestID] = append(requestIDToTimes[log.RequestID], log.Time)
			}
		}

		// Check for duplicate keys
		duplicateKeys := []string{}
		for key, count := range keyCount {
			if count > 1 {
				duplicateKeys = append(duplicateKeys, fmt.Sprintf("%s (x%d)", key, count))
			}
		}

		// Check for duplicate request IDs
		duplicateRequestIDs := []string{}
		for reqID, count := range requestIDCount {
			if count > 1 {
				duplicateRequestIDs = append(duplicateRequestIDs, fmt.Sprintf("%s (x%d)", reqID, count))
			}
		}

		// Report findings
		By(fmt.Sprintf("found %d batch PUT operations (expected %d)", batchPutCount, numOperations))
		GinkgoWriter.Printf("\n=== Large Batch Test Analysis ===\n")
		GinkgoWriter.Printf("Total batch operations logged: %d\n", batchPutCount)
		GinkgoWriter.Printf("Expected operations: %d\n", numOperations)
		GinkgoWriter.Printf("Difference: %d (%.1f%%)\n",
			batchPutCount-numOperations,
			float64(batchPutCount-numOperations)/float64(numOperations)*100)
		GinkgoWriter.Printf("Unique keys logged: %d\n", len(keyCount))
		GinkgoWriter.Printf("Unique request IDs: %d\n", len(requestIDCount))

		if len(duplicateKeys) > 0 {
			GinkgoWriter.Printf("\n!!! DUPLICATE KEYS DETECTED !!!\n")
			GinkgoWriter.Printf("Number of keys with duplicates: %d\n", len(duplicateKeys))
			GinkgoWriter.Printf("First 10 duplicate keys:\n")
			for i, key := range duplicateKeys {
				if i >= 10 {
					GinkgoWriter.Printf("... and %d more\n", len(duplicateKeys)-10)
					break
				}
				GinkgoWriter.Printf("  - %s\n", key)
			}
		}

		if len(duplicateRequestIDs) > 0 {
			GinkgoWriter.Printf("\n!!! DUPLICATE REQUEST IDS DETECTED !!!\n")
			GinkgoWriter.Printf("Number of request IDs with duplicates: %d\n", len(duplicateRequestIDs))
			GinkgoWriter.Printf("First 10 duplicate request IDs with timestamps:\n")
			count := 0
			for reqID, dupCount := range requestIDCount {
				if dupCount > 1 && count < 10 {
					times := requestIDToTimes[reqID]
					GinkgoWriter.Printf("  - %s (x%d)\n", reqID, dupCount)
					for i, t := range times {
						GinkgoWriter.Printf("      [%d] %s\n", i+1, t.Format("15:04:05.000"))
					}
					if len(times) > 1 {
						timeDiff := times[1].Sub(times[0])
						GinkgoWriter.Printf("      Time between duplicates: %v\n", timeDiff)
					}
					count++
				}
			}
			if len(duplicateRequestIDs) > 10 {
				GinkgoWriter.Printf("... and %d more\n", len(duplicateRequestIDs)-10)
			}
		}

		GinkgoWriter.Printf("================================\n\n")

		// Fail with detailed message if we have duplicates
		if len(duplicateKeys) > 0 || len(duplicateRequestIDs) > 0 {
			Fail(fmt.Sprintf("Duplicate logging detected! "+
				"Expected %d operations but got %d. "+
				"%d keys duplicated, %d request IDs duplicated. "+
				"This indicates a bug in offset tracking or batch processing.",
				numOperations, batchPutCount, len(duplicateKeys), len(duplicateRequestIDs)))
		}

		Expect(batchPutCount).To(Equal(numOperations),
			"Should have exactly %d batch PUT operations (no duplicates)", numOperations)

		// Verify logs are in chronological order
		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("triggers processing by time threshold (below count threshold)", func() {
		// Config has count-threshold: 2, time-threshold: 2s
		// Create only 1 operation (below count threshold)
		testKey := "time-threshold-test.txt"
		testContent := []byte("time threshold test data")

		By("performing single PUT operation (below count threshold)")
		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		// Wait for log to appear - should be triggered by time threshold
		By("waiting for log to appear (should trigger by time threshold)")
		startWait := time.Now()
		logs := waitForLogCount(ctx, 1)
		waitDuration := time.Since(startWait)

		// Verify the operation was logged
		By("verifying the operation was logged")
		Expect(len(logs)).To(BeNumerically(">=", 1),
			"Should have logged at least 1 operation")

		// Find our PUT operation
		var foundPut bool
		for _, log := range logs {
			if log.Operation == "REST.PUT.OBJECT" &&
				log.Bucket == ctx.SourceBucket &&
				log.Key == testKey {
				foundPut = true
				break
			}
		}
		Expect(foundPut).To(BeTrue(), "Should have found the PUT operation in logs")

		// Verify it took at least the time threshold to process
		// (allowing some tolerance for processing overhead)
		By(fmt.Sprintf("verifying time threshold was used (waited %v)", waitDuration))
		// The time threshold should trigger within a reasonable window
		// With discovery interval of 1s and time threshold of 2s,
		// we expect logs to appear within ~3-5 seconds
		Expect(waitDuration).To(BeNumerically(">=", 1*time.Second),
			"Should wait at least 1 second for time threshold")
		Expect(waitDuration).To(BeNumerically("<", 10*time.Second),
			"Should not take more than 10 seconds")
	})

	It("maintains offset persistence across cycles without duplicates", func() {
		const firstBatchSize = 5
		const secondBatchSize = 5
		const totalExpected = firstBatchSize + secondBatchSize

		// First batch of operations
		By(fmt.Sprintf("performing first batch of %d PUT operations", firstBatchSize))
		for i := 0; i < firstBatchSize; i++ {
			testKey := fmt.Sprintf("offset-test-batch1-%d.txt", i)
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("batch 1 data %d", i))),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT operation %d (batch 1) should succeed", i)
		}

		// Wait for first batch to be processed
		By(fmt.Sprintf("waiting for first batch of %d logs", firstBatchSize))
		firstBatchLogs := waitForLogCount(ctx, firstBatchSize)
		Expect(len(firstBatchLogs)).To(BeNumerically(">=", firstBatchSize),
			"Should have logged first batch")

		// Give a moment for offset to be committed
		By("waiting for offset to be committed")
		time.Sleep(3 * time.Second)

		// Record timestamp before second batch
		secondBatchStart := time.Now()

		// Second batch of operations
		By(fmt.Sprintf("performing second batch of %d PUT operations", secondBatchSize))
		for i := 0; i < secondBatchSize; i++ {
			testKey := fmt.Sprintf("offset-test-batch2-%d.txt", i)
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("batch 2 data %d", i))),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT operation %d (batch 2) should succeed", i)
		}

		// Wait for second batch to be processed
		By(fmt.Sprintf("waiting for second batch of %d logs", secondBatchSize))
		time.Sleep(5 * time.Second) // Give time for second batch to be processed

		// Fetch all logs since test start
		By("fetching all logs since test start")
		allLogs, err := fetchAllLogsSince(ctx, ctx.TestStartTime)
		Expect(err).NotTo(HaveOccurred(), "Should fetch all logs")

		// Count PUT operations on our source bucket
		By("counting all PUT operations")
		putOps := 0
		batch1Ops := 0
		batch2Ops := 0
		requestIDs := make(map[string]int)

		for _, log := range allLogs {
			if log.Operation == "REST.PUT.OBJECT" && log.Bucket == ctx.SourceBucket {
				putOps++
				requestIDs[log.RequestID]++

				// Count by batch based on key pattern
				if len(log.Key) > 0 {
					if len(log.Key) >= 18 && log.Key[:18] == "offset-test-batch1" {
						batch1Ops++
					} else if len(log.Key) >= 18 && log.Key[:18] == "offset-test-batch2" {
						batch2Ops++
					}
				}
			}
		}

		// Verify total count matches expected (no duplicates)
		By(fmt.Sprintf("verifying total operation count (expected %d, got %d)", totalExpected, putOps))
		Expect(putOps).To(Equal(totalExpected),
			"Should have exactly %d PUT operations (no duplicates)", totalExpected)

		// Verify both batches were logged
		By(fmt.Sprintf("verifying batch 1 operations (expected %d, got %d)", firstBatchSize, batch1Ops))
		Expect(batch1Ops).To(Equal(firstBatchSize),
			"Should have all batch 1 operations")

		By(fmt.Sprintf("verifying batch 2 operations (expected %d, got %d)", secondBatchSize, batch2Ops))
		Expect(batch2Ops).To(Equal(secondBatchSize),
			"Should have all batch 2 operations")

		// Verify no duplicate request IDs (each operation should appear exactly once)
		By("verifying no duplicate request IDs")
		for reqID, count := range requestIDs {
			Expect(count).To(Equal(1),
				"Request ID %s should appear exactly once, but appeared %d times", reqID, count)
		}

		// Verify second batch logs appear after second batch start time
		By("verifying second batch logs have correct timestamps")
		batch2LogsAfterStart := 0
		for _, log := range allLogs {
			if log.Operation == "REST.PUT.OBJECT" &&
				len(log.Key) >= 18 &&
				log.Key[:18] == "offset-test-batch2" &&
				log.Time.After(secondBatchStart) {
				batch2LogsAfterStart++
			}
		}
		Expect(batch2LogsAfterStart).To(Equal(secondBatchSize),
			"Second batch logs should have timestamps after second batch start")

		// Verify logs are in chronological order
		By("verifying all logs are in chronological order")
		verifyChronologicalOrder(allLogs)
	})
})

// waitForLogCountWithTimeout waits for at least expectedCount logs with a custom timeout
func waitForLogCountWithTimeout(ctx *E2ETestContext, expectedCount int, timeout time.Duration) []*ParsedLogRecord {
	var allLogs []*ParsedLogRecord

	Eventually(func() int {
		logs, err := fetchAllLogsSince(ctx, ctx.TestStartTime)
		if err != nil {
			return 0
		}
		allLogs = logs
		return len(logs)
	}, timeout, logPollInterval).Should(BeNumerically(">=", expectedCount),
		"Expected at least %d logs within %v", expectedCount, timeout)

	return allLogs
}
