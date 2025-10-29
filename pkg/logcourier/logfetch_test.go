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

var _ = Describe("LogFetcher", func() {
	var (
		ctx    context.Context
		helper *testutil.ClickHouseTestHelper
		fetcher *logcourier.LogFetcher
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		helper, err = testutil.NewClickHouseTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = helper.SetupSchema(ctx)
		Expect(err).NotTo(HaveOccurred())

		fetcher = logcourier.NewLogFetcher(helper.Client, helper.DatabaseName)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("FetchLogs", func() {
		It("should fetch logs in time window", func() {
			now := time.Now()

			for i := 0; i < 3; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      now.Add(time.Duration(i) * time.Second),
					ReqID:          fmt.Sprintf("req-%d", i),
					Action:         "GetObject",
					ObjectKey:      fmt.Sprintf("key-%d", i),
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Wait for materialized view to process records
			query := fmt.Sprintf("SELECT count() FROM %s.access_logs WHERE bucketName = 'test-bucket'", helper.DatabaseName)
			err := helper.WaitForMaterializedView(ctx, query, 0)
			Expect(err).NotTo(HaveOccurred())

			batch := logcourier.LogBatch{
				Bucket:       "test-bucket",
				MinTimestamp: now.Add(-1 * time.Second),
				MaxTimestamp: now.Add(10 * time.Second),
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(3))
		})

		It("should return logs sorted by timestamp", func() {
			now := time.Now()

			// Insert logs in reverse chronological order
			times := []time.Time{
				now.Add(3 * time.Second),
				now.Add(1 * time.Second),
				now.Add(2 * time.Second),
			}

			for i, t := range times {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      t,
					ReqID:          fmt.Sprintf("req-%d", i),
					Action:         "GetObject",
					ObjectKey:      fmt.Sprintf("key-%d", i),
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Wait for materialized view
			query := fmt.Sprintf("SELECT count() FROM %s.access_logs WHERE bucketName = 'test-bucket'", helper.DatabaseName)
			err := helper.WaitForMaterializedView(ctx, query, 0)
			Expect(err).NotTo(HaveOccurred())

			batch := logcourier.LogBatch{
				Bucket:       "test-bucket",
				MinTimestamp: now,
				MaxTimestamp: now.Add(10 * time.Second),
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(3))

			// Verify sorted by timestamp (ascending)
			for i := 1; i < len(records); i++ {
				Expect(records[i].Timestamp.After(records[i-1].Timestamp) ||
					records[i].Timestamp.Equal(records[i-1].Timestamp)).To(BeTrue(),
					"records should be sorted by timestamp in ascending order")
			}
		})

		It("should only fetch logs for specified bucket", func() {
			now := time.Now()

			// Insert logs for bucket-1
			err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
				LoggingEnabled: true,
				BucketName:     "bucket-1",
				Timestamp:      now,
				ReqID:          "req-1",
				Action:         "GetObject",
				ObjectKey:      "key1",
			})
			Expect(err).NotTo(HaveOccurred())

			// Insert logs for bucket-2
			err = helper.InsertTestLog(ctx, testutil.TestLogRecord{
				LoggingEnabled: true,
				BucketName:     "bucket-2",
				Timestamp:      now,
				ReqID:          "req-2",
				Action:         "GetObject",
				ObjectKey:      "key2",
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for materialized view
			query := fmt.Sprintf("SELECT count() FROM %s.access_logs", helper.DatabaseName)
			err = helper.WaitForMaterializedView(ctx, query, 0)
			Expect(err).NotTo(HaveOccurred())

			// Fetch only bucket-1
			batch := logcourier.LogBatch{
				Bucket:       "bucket-1",
				MinTimestamp: now.Add(-1 * time.Second),
				MaxTimestamp: now.Add(1 * time.Second),
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1))
			Expect(records[0].BucketName).To(Equal("bucket-1"))
		})

		It("should filter by insertedAt time window", func() {
			now := time.Now()

			// Insert log that will be outside the window
			err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
				LoggingEnabled: true,
				BucketName:     "test-bucket",
				Timestamp:      now,
				ReqID:          "req-old",
				Action:         "GetObject",
				ObjectKey:      "key-old",
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for it to be processed
			query := fmt.Sprintf("SELECT count() FROM %s.access_logs WHERE bucketName = 'test-bucket'", helper.DatabaseName)
			err = helper.WaitForMaterializedView(ctx, query, 0)
			Expect(err).NotTo(HaveOccurred())

			// Query with a future time window (should get no results)
			futureTime := now.Add(1 * time.Hour)
			batch := logcourier.LogBatch{
				Bucket:       "test-bucket",
				MinTimestamp: futureTime,
				MaxTimestamp: futureTime.Add(1 * time.Hour),
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(BeEmpty())
		})

		It("should return empty list when no logs match", func() {
			batch := logcourier.LogBatch{
				Bucket:       "nonexistent-bucket",
				MinTimestamp: time.Now().Add(-1 * time.Hour),
				MaxTimestamp: time.Now(),
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(BeEmpty())
		})

		It("should fetch all required fields", func() {
			now := time.Now()

			// Insert a log with specific field values
			err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
				LoggingEnabled: true,
				BucketName:     "test-bucket",
				Timestamp:      now,
				ReqID:          "test-req-id",
				Action:         "PutObject",
				ObjectKey:      "test-key",
				BytesSent: 12345,
				HttpCode:  200,
			})
			Expect(err).NotTo(HaveOccurred())

			// Wait for materialized view
			query := fmt.Sprintf("SELECT count() FROM %s.access_logs WHERE bucketName = 'test-bucket'", helper.DatabaseName)
			err = helper.WaitForMaterializedView(ctx, query, 0)
			Expect(err).NotTo(HaveOccurred())

			batch := logcourier.LogBatch{
				Bucket:       "test-bucket",
				MinTimestamp: now.Add(-1 * time.Second),
				MaxTimestamp: now.Add(1 * time.Second),
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1))

			rec := records[0]
			Expect(rec.BucketName).To(Equal("test-bucket"))
			Expect(rec.ReqID).To(Equal("test-req-id"))
			Expect(rec.Action).To(Equal("PutObject"))
			Expect(rec.ObjectKey).To(Equal("test-key"))
			Expect(rec.BytesSent).To(Equal(uint64(12345)))
			Expect(rec.HttpCode).To(Equal(uint16(200)))
		})
	})
})
