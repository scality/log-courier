package logcourier_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("BatchFinder", func() {
	var (
		ctx    context.Context
		helper *testutil.ClickHouseTestHelper
		finder *logcourier.BatchFinder
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		helper, err = testutil.NewClickHouseTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = helper.SetupSchema(ctx)
		Expect(err).NotTo(HaveOccurred())

		finder = logcourier.NewBatchFinder(helper.Client, helper.DatabaseName, 10, 60)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("FindBatches", func() {
		It("should return empty list when no logs", func() {
			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(BeEmpty())
		})

		It("should return log batch when count threshold met", func() {
			// Insert 15 logs (threshold is 10)
			for i := 0; i < 15; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      time.Now(),
					ReqID:          fmt.Sprintf("req-%d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].Bucket).To(Equal("test-bucket"))
			Expect(batches[0].LogCount).To(BeNumerically(">=", 15))
		})

		It("should return log batch when time threshold met", func() {
			// Insert 1 log directly into access_logs with old insertedAt timestamp
			// This bypasses the materialized view to set insertedAt manually
			oldTime := time.Now().Add(-2 * time.Hour)
			query := fmt.Sprintf(`
				INSERT INTO %s.access_logs
				(bucketOwner, bucketName, startTime, clientIP, requester, req_id, operation,
				 objectKey, requestURI, httpCode, errorCode, bytesSent, objectSize, totalTime,
				 turnAroundTime, referer, userAgent, versionId, signatureVersion, cipherSuite,
				 authenticationType, hostHeader, tlsVersion, aclRequired, timestamp, insertedAt,
				 loggingTargetBucket, loggingTargetPrefix, raftSessionID)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName)
			err := helper.Client.Exec(ctx, query,
				"",                 // bucketOwner
				"test-bucket",      // bucketName
				time.Now(),         // startTime
				"",                 // clientIP
				"",                 // requester
				"req-old",          // req_id
				"GetObject",        // operation
				"",                 // objectKey
				"/test-bucket/key", // requestURI
				uint16(0),          // httpCode
				"",                 // errorCode
				uint64(0),          // bytesSent
				uint64(0),          // objectSize
				float32(0),         // totalTime
				float32(0),         // turnAroundTime
				"",                 // referer
				"",                 // userAgent
				"",                 // versionId
				"",                 // signatureVersion
				"",                 // cipherSuite
				"",                 // authenticationType
				"",                 // hostHeader
				"",                 // tlsVersion
				"",                 // aclRequired
				time.Now(),         // timestamp
				oldTime,            // insertedAt (old)
				"",                 // loggingTargetBucket
				"",                 // loggingTargetPrefix
				uint16(0),          // raftSessionID
			)
			Expect(err).NotTo(HaveOccurred())

			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].Bucket).To(Equal("test-bucket"))
		})

		It("should not return batches when neither threshold met", func() {
			// Insert 5 logs (below count threshold of 10)
			// With recent timestamp (within time threshold of 60 seconds)
			for i := 0; i < 5; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      time.Now(),
					ReqID:          fmt.Sprintf("req-%d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(BeEmpty())
		})

		It("should respect bucket offsets", func() {
			// Insert logs
			for i := 0; i < 15; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      time.Now(),
					ReqID:          fmt.Sprintf("req-%d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Commit offset at current time
			offsetTime := time.Now()
			offsetQuery := fmt.Sprintf("INSERT INTO %s.offsets (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedTimestamp, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)", helper.DatabaseName)
			err := helper.Client.Exec(ctx, offsetQuery,
				"test-bucket", uint16(0), offsetTime, offsetTime, "req-999")
			Expect(err).NotTo(HaveOccurred())

			// Should not return batches (all logs processed)
			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(BeEmpty())
		})

		It("should handle multiple buckets", func() {
			// Insert logs for bucket-1 (above threshold)
			for i := 0; i < 15; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "bucket-1",
					Timestamp:      time.Now(),
					ReqID:          fmt.Sprintf("req-1-%d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Insert logs for bucket-2 (above threshold)
			for i := 0; i < 20; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "bucket-2",
					Timestamp:      time.Now(),
					ReqID:          fmt.Sprintf("req-2-%d", i),
					Action:         "PutObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(HaveLen(2))

			buckets := []string{batches[0].Bucket, batches[1].Bucket}
			Expect(buckets).To(ConsistOf("bucket-1", "bucket-2"))
		})

		It("should respect composite offset", func() {
			// Use old time to trigger time threshold
			oldTime := time.Now().Add(-2 * time.Hour)
			insertedAt := oldTime.Truncate(time.Second)
			timestamp := oldTime

			// Insert 5 logs directly with old insertedAt: same insertedAt and timestamp, different req_ids
			for i := 0; i < 5; i++ {
				query := fmt.Sprintf(`
					INSERT INTO %s.access_logs
					(insertedAt, bucketName, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				`, helper.DatabaseName)
				err := helper.Client.Exec(ctx, query,
					insertedAt,                 // insertedAt (old, same for all)
					"test-bucket",              // bucketName
					timestamp,                  // timestamp (same for all)
					fmt.Sprintf("req-%03d", i), // req_id (different)
					"GetObject",                // operation
					true,                       // loggingEnabled
					uint16(0),                  // raftSessionID
					"/test-bucket/key",         // requestURI
				)
				Expect(err).NotTo(HaveOccurred())
			}

			time.Sleep(100 * time.Millisecond)

			// Set offset to req-002 (skip first 3 logs)
			offsetQuery := fmt.Sprintf(
				"INSERT INTO %s.offsets (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedTimestamp, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)",
				helper.DatabaseName)
			err := helper.Client.Exec(ctx, offsetQuery,
				"test-bucket", uint16(0),
				insertedAt,
				timestamp,
				"req-002")
			Expect(err).NotTo(HaveOccurred())

			// BatchFinder should only find logs after composite offset
			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Should find 1 batch with 2 logs (req-003, req-004)
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].LogCount).To(BeNumerically(">=", 2))
		})

		It("should not reprocess logs when insertedAt order differs from timestamp order", func() {
			// - Log A arrives quickly (insertedAt=10:00:03, timestamp=10:00:02)
			// - Log B arrives delayed (insertedAt=10:00:05, timestamp=10:00:01)
			// - Next cycle should not re-discover log B

			baseTime := time.Now().Add(-1 * time.Hour)

			// Insert Log A: operation at :02, arrives at :03
			logATimestamp := baseTime.Add(2 * time.Second)
			logAInsertedAt := baseTime.Add(3 * time.Second)
			query := fmt.Sprintf(`
				INSERT INTO %s.access_logs
				(insertedAt, bucketName, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName)
			err := helper.Client.Exec(ctx, query,
				logAInsertedAt, "test-bucket", logATimestamp, "req-A",
				"GetObject", true, uint16(0), "/test-bucket/key-a")
			Expect(err).NotTo(HaveOccurred())

			// Insert Log B: operation at :01, arrives at :05
			logBTimestamp := baseTime.Add(1 * time.Second)
			logBInsertedAt := baseTime.Add(5 * time.Second)
			err = helper.Client.Exec(ctx, query,
				logBInsertedAt, "test-bucket", logBTimestamp, "req-B",
				"GetObject", true, uint16(0), "/test-bucket/key-b")
			Expect(err).NotTo(HaveOccurred())

			// Cycle 1: Find batches
			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].LogCount).To(BeNumerically(">=", 2))

			// Cycle 1: Fetch logs and simulate processing
			fetcher := logcourier.NewLogFetcher(helper.Client, helper.DatabaseName)
			records, err := fetcher.FetchLogs(ctx, batches[0])
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(2))

			// Cycle 1: Commit offset from last fetched log
			// (simulating what Processor does)
			lastLog := records[len(records)-1]
			offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
			err = offsetMgr.CommitOffset(ctx, "test-bucket", uint16(0), logcourier.Offset{
				InsertedAt: lastLog.InsertedAt,
				Timestamp:  lastLog.Timestamp,
				ReqID:      lastLog.ReqID,
			})
			Expect(err).NotTo(HaveOccurred())

			// Cycle 2: Find batches again
			batches, err = finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(batches).To(BeEmpty(), "Expected no batches after processing all logs, but found %d batch(es)", len(batches))
		})
	})
})
