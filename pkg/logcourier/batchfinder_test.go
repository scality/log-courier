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
				(insertedAt, bucketName, timestamp, req_id, action, loggingEnabled, raftSessionId, httpURL)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName)
			err := helper.Client.Exec(ctx, query,
				oldTime,           // insertedAt (old)
				"test-bucket",     // bucketName
				time.Now(),        // timestamp
				"req-old",         // req_id
				"GetObject",       // action
				true,              // loggingEnabled
				uint16(0),         // raftSessionId
				"/test-bucket/key", // httpURL
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
			offsetQuery := fmt.Sprintf("INSERT INTO %s.offsets (bucketName, raftSessionId, last_processed_ts) VALUES (?, ?, ?)", helper.DatabaseName)
			err := helper.Client.Exec(ctx, offsetQuery,
				"test-bucket", uint16(0), offsetTime)
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
	})
})
