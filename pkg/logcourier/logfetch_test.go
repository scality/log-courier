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

		om := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
		fetcher = logcourier.NewLogFetcher(helper.Client, helper.DatabaseName, om)
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

		It("should fetch log with all fields", func() {
			insertTime := time.Now()
			reqTime := insertTime.Add(-5 * time.Minute)

			err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
				Timestamp:      reqTime,
				BucketName:     "test-bucket",
				ReqID:          "test-req-id",
				Action:         "PutObject",
				ObjectKey:      "test-key",
				BytesSent:      12345,
				HttpCode:       200,
				RaftSessionID:  42,
				LoggingEnabled: true,
			})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(100 * time.Millisecond)

			batch := logcourier.LogBatch{
				Bucket:        "test-bucket",
				RaftSessionID: 42,
				MinTimestamp:  reqTime.Add(-1 * time.Hour),
				MaxTimestamp:  reqTime.Add(1 * time.Hour),
				LogCount:      1,
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(records)).To(Equal(1))

			rec := records[0]
			Expect(rec.BucketName).To(Equal("test-bucket"))
			Expect(rec.ReqID).To(Equal("test-req-id"))
			Expect(rec.ObjectKey).To(Equal("test-key"))
			Expect(rec.BytesSent).To(Equal(uint64(12345)))
			Expect(rec.HttpCode).To(Equal(uint16(200)))
			Expect(rec.RaftSessionID).To(Equal(uint16(42)))
			Expect(rec.InsertedAt).NotTo(BeZero()) // NEW: Verify insertedAt is set
			Expect(rec.Timestamp).NotTo(BeZero())   // Verify timestamp
		})

		It("should filter by triple composite offset", func() {
			baseTime := time.Now()
			insertedAt := baseTime.Truncate(time.Second)
			timestamp := baseTime

			// Insert 5 logs with same insertedAt and timestamp
			for i := 0; i < 5; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					Timestamp:      timestamp,
					BucketName:     "test-bucket",
					ReqID:          fmt.Sprintf("req-%03d", i),
					Action:         "GetObject",
					LoggingEnabled: true,
					RaftSessionID:  0,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			time.Sleep(100 * time.Millisecond)

			// Set offset to skip first 3 logs (req-000, req-001, req-002)
			offsetQuery := fmt.Sprintf(
				"INSERT INTO %s.offsets (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedTimestamp, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)",
				helper.DatabaseName)
			err := helper.Client.Exec(ctx, offsetQuery,
				"test-bucket", uint16(0),
				insertedAt,
				timestamp,
				"req-002")
			Expect(err).NotTo(HaveOccurred())

			// Create offset manager and fetcher
			om := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
			fetcher := logcourier.NewLogFetcher(helper.Client, helper.DatabaseName, om)

			batch := logcourier.LogBatch{
				Bucket:        "test-bucket",
				RaftSessionID: 0,
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())

			// Should only get logs after offset (req-003, req-004)
			Expect(len(records)).To(Equal(2))
			Expect(records[0].ReqID).To(Equal("req-003"))
			Expect(records[1].ReqID).To(Equal("req-004"))
		})
	})
})
