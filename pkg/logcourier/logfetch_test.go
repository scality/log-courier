package logcourier_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/clickhouse"
	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("LogFetcher", func() {
	var (
		ctx     context.Context
		helper  *testutil.ClickHouseTestHelper
		fetcher *logcourier.LogFetcher
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		helper, err = testutil.NewClickHouseTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = helper.SetupSchema(ctx)
		Expect(err).NotTo(HaveOccurred())

		fetcher = logcourier.NewLogFetcher(helper.Client(), helper.DatabaseName, 5)
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
				Bucket:              "test-bucket",
				LastProcessedOffset: logcourier.Offset{},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(3))
		})

		It("should return logs sorted by (insertedAt, startTime, reqID)", func() {
			baseTime := time.Now().Add(-1 * time.Hour)

			// Insert logs with different insertedAt and timestamp values to test full ordering
			// Log C: Latest insertedAt (should be fetched last)
			// Log A: Middle insertedAt, early timestamp
			// Log B: Middle insertedAt, late timestamp (should come after A due to timestamp)
			// Log D: Earliest insertedAt (should be fetched first)

			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

			// Log D: insertedAt=1s, timestamp=4s (earliest insertedAt)
			err := helper.Client().Exec(ctx, query,
				baseTime.Add(1*time.Second), "test-bucket", baseTime.Add(4*time.Second), "req-D",
				"GetObject", true, uint16(0), "/test-bucket/key-d")
			Expect(err).NotTo(HaveOccurred())

			// Log A: insertedAt=3s, timestamp=1s
			err = helper.Client().Exec(ctx, query,
				baseTime.Add(3*time.Second), "test-bucket", baseTime.Add(1*time.Second), "req-A",
				"GetObject", true, uint16(0), "/test-bucket/key-a")
			Expect(err).NotTo(HaveOccurred())

			// Log B: insertedAt=3s, timestamp=2s (same insertedAt as A, later timestamp)
			err = helper.Client().Exec(ctx, query,
				baseTime.Add(3*time.Second), "test-bucket", baseTime.Add(2*time.Second), "req-B",
				"GetObject", true, uint16(0), "/test-bucket/key-b")
			Expect(err).NotTo(HaveOccurred())

			// Log C: insertedAt=5s, timestamp=3s (latest insertedAt)
			err = helper.Client().Exec(ctx, query,
				baseTime.Add(5*time.Second), "test-bucket", baseTime.Add(3*time.Second), "req-C",
				"GetObject", true, uint16(0), "/test-bucket/key-c")
			Expect(err).NotTo(HaveOccurred())

			batch := logcourier.LogBatch{
				Bucket:              "test-bucket",
				LastProcessedOffset: logcourier.Offset{},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(4))

			// Verify sorted by (insertedAt, timestamp, reqID)
			for i := 1; i < len(records); i++ {
				prev := records[i-1]
				curr := records[i]

				if curr.InsertedAt.After(prev.InsertedAt) {
					continue
				} else if curr.InsertedAt.Equal(prev.InsertedAt) {
					if curr.Timestamp.After(prev.Timestamp) || curr.Timestamp.Equal(prev.Timestamp) {
						if curr.Timestamp.Equal(prev.Timestamp) {
							Expect(curr.ReqID > prev.ReqID).To(BeTrue(),
								"When insertedAt and timestamp are equal, reqID should increase")
						}
						continue
					}
				}
				// If we get here, ordering is violated
				Expect(true).To(BeFalse(),
					"Records should be sorted by (insertedAt, timestamp, reqID). "+
						"Record %d: (%s, %s, %s), Record %d: (%s, %s, %s)",
					i-1, prev.InsertedAt, prev.Timestamp, prev.ReqID,
					i, curr.InsertedAt, curr.Timestamp, curr.ReqID)
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
				Bucket:              "bucket-1",
				LastProcessedOffset: logcourier.Offset{},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1))
			Expect(records[0].BucketName).To(Equal("bucket-1"))
		})

		It("should return empty list when no logs match", func() {
			batch := logcourier.LogBatch{
				Bucket:              "nonexistent-bucket",
				LastProcessedOffset: logcourier.Offset{},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(BeEmpty())
		})

		It("should only fetch logs after LastProcessedOffset", func() {
			baseTime := time.Now().Add(-1 * time.Hour)

			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

			// Insert 3 logs with sequential insertedAt and timestamps
			err := helper.Client().Exec(ctx, query,
				baseTime.Add(1*time.Second), "test-bucket", baseTime.Add(1*time.Second), "req-001",
				"GetObject", true, uint16(0), "/test-bucket/key-1")
			Expect(err).NotTo(HaveOccurred())

			err = helper.Client().Exec(ctx, query,
				baseTime.Add(2*time.Second), "test-bucket", baseTime.Add(2*time.Second), "req-002",
				"GetObject", true, uint16(0), "/test-bucket/key-2")
			Expect(err).NotTo(HaveOccurred())

			err = helper.Client().Exec(ctx, query,
				baseTime.Add(3*time.Second), "test-bucket", baseTime.Add(3*time.Second), "req-003",
				"GetObject", true, uint16(0), "/test-bucket/key-3")
			Expect(err).NotTo(HaveOccurred())

			// Set offset to 2nd log - should only fetch 3rd log
			batch := logcourier.LogBatch{
				Bucket: "test-bucket",
				LastProcessedOffset: logcourier.Offset{
					InsertedAt: baseTime.Add(2 * time.Second),
					StartTime:  baseTime.Add(2 * time.Second),
					ReqID:      "req-002",
				},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1), "Should only fetch logs after offset")
			Expect(records[0].ReqID).To(Equal("req-003"), "Should fetch the 3rd log only")
		})

		It("should filter logs with same insertedAt using composite offset", func() {
			// This test verifies the fix for LOGC-32 at the LogFetcher level
			baseTime := time.Now().Add(-1 * time.Hour)
			insertedAt := baseTime.Truncate(time.Second) // Same for all logs

			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, startTime, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

			// Insert 3 logs: same insertedAt, different startTime
			startTimeA := baseTime.Add(1 * time.Second)
			err := helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", startTimeA, startTimeA, "req-A",
				"GetObject", true, uint16(0), "/test-bucket/key-a")
			Expect(err).NotTo(HaveOccurred())

			startTimeB := baseTime.Add(2 * time.Second)
			err = helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", startTimeB, startTimeB, "req-B",
				"GetObject", true, uint16(0), "/test-bucket/key-b")
			Expect(err).NotTo(HaveOccurred())

			startTimeC := baseTime.Add(3 * time.Second)
			err = helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", startTimeC, startTimeC, "req-C",
				"GetObject", true, uint16(0), "/test-bucket/key-c")
			Expect(err).NotTo(HaveOccurred())

			// Set offset to log B (middle log)
			batch := logcourier.LogBatch{
				Bucket: "test-bucket",
				LastProcessedOffset: logcourier.Offset{
					InsertedAt: insertedAt,
					StartTime:  baseTime.Add(2 * time.Second),
					ReqID:      "req-B",
				},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1), "Should only fetch log C (after offset B)")
			Expect(records[0].ReqID).To(Equal("req-C"), "Should fetch only the log with startTime > offset startTime")

			// Verify logs A and B were excluded
			for _, rec := range records {
				Expect(rec.ReqID).NotTo(Equal("req-A"), "Log A should be excluded (startTime < offset)")
				Expect(rec.ReqID).NotTo(Equal("req-B"), "Log B should be excluded (matches offset exactly)")
			}
		})

		It("should exclude log that exactly matches offset", func() {
			baseTime := time.Now().Add(-1 * time.Hour)

			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

			// Insert a single log
			insertedAt := baseTime.Add(1 * time.Second)
			timestamp := baseTime.Add(1 * time.Second)
			reqID := "req-exact"

			err := helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", timestamp, reqID,
				"GetObject", true, uint16(0), "/test-bucket/key")
			Expect(err).NotTo(HaveOccurred())

			// Set offset to exact same log
			batch := logcourier.LogBatch{
				Bucket: "test-bucket",
				LastProcessedOffset: logcourier.Offset{
					InsertedAt: insertedAt,
					StartTime:  timestamp,
					ReqID:      reqID,
				},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(BeEmpty(), "Should not fetch log that exactly matches offset (not > itself)")
		})

		It("should filter by reqID when insertedAt and startTime are equal", func() {
			baseTime := time.Now().Add(-1 * time.Hour)
			insertedAt := baseTime.Truncate(time.Second)
			startTime := baseTime.Truncate(time.Second)

			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, startTime, timestamp, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

			// Insert 3 logs: same insertedAt, same startTime, different reqIDs
			err := helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", startTime, startTime, "req-A",
				"GetObject", true, uint16(0), "/test-bucket/key-a")
			Expect(err).NotTo(HaveOccurred())

			err = helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", startTime, startTime, "req-B",
				"GetObject", true, uint16(0), "/test-bucket/key-b")
			Expect(err).NotTo(HaveOccurred())

			err = helper.Client().Exec(ctx, query,
				insertedAt, "test-bucket", startTime, startTime, "req-C",
				"GetObject", true, uint16(0), "/test-bucket/key-c")
			Expect(err).NotTo(HaveOccurred())

			// Set offset to req-B (middle reqID)
			batch := logcourier.LogBatch{
				Bucket: "test-bucket",
				LastProcessedOffset: logcourier.Offset{
					InsertedAt: insertedAt,
					StartTime:  startTime,
					ReqID:      "req-B",
				},
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1), "Should only fetch logs with reqID > offset reqID")
			Expect(records[0].ReqID).To(Equal("req-C"), "Should fetch only req-C (reqID > 'req-B')")

			// Verify A and B were excluded
			for _, rec := range records {
				Expect(rec.ReqID > "req-B").To(BeTrue(), "All fetched logs should have reqID > offset reqID")
			}
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
				RaftSessionID:  0,
				LoggingEnabled: true,
			})
			Expect(err).NotTo(HaveOccurred())

			time.Sleep(100 * time.Millisecond)

			batch := logcourier.LogBatch{
				Bucket:              "test-bucket",
				RaftSessionID:       0,
				LastProcessedOffset: logcourier.Offset{},
				LogCount:            1,
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(1))

			rec := records[0]
			Expect(rec.BucketName).To(Equal("test-bucket"))
			Expect(rec.ReqID).To(Equal("test-req-id"))
			Expect(*rec.Operation).To(Equal("PutObject"))
			Expect(*rec.ObjectKey).To(Equal("test-key"))
			Expect(*rec.BytesSent).To(Equal(uint64(12345)))
			Expect(*rec.HttpCode).To(Equal(uint16(200)))
			Expect(rec.RaftSessionID).To(Equal(uint16(0)))
			Expect(rec.InsertedAt).NotTo(BeZero())
			Expect(rec.Timestamp).NotTo(BeZero())
		})

		It("should respect maxLogsPerBatch limit", func() {
			baseTime := time.Now().Add(-1 * time.Hour)
			for i := 0; i < 6; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      baseTime.Add(time.Duration(i) * time.Second),
					ReqID:          fmt.Sprintf("req-%03d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			batch := logcourier.LogBatch{
				Bucket:              "test-bucket",
				LastProcessedOffset: logcourier.Offset{},
				LogCount:            5,
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())

			Expect(records).To(HaveLen(5))

			Expect(records[0].ReqID).To(Equal("req-000"))
			Expect(records[4].ReqID).To(Equal("req-004"))
		})

		It("should order by insertedAt", func() {
			// Insert logs with different timestamps
			baseTime := time.Now().Add(-1 * time.Hour)
			for i := 0; i < 5; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "test-bucket",
					Timestamp:      baseTime.Add(time.Duration(i) * time.Second),
					ReqID:          fmt.Sprintf("req-%03d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			batch := logcourier.LogBatch{
				Bucket:              "test-bucket",
				LastProcessedOffset: logcourier.Offset{},
				LogCount:            10,
			}

			records, err := fetcher.FetchLogs(ctx, batch)
			Expect(err).NotTo(HaveOccurred())

			// Verify chronological order by checking insertedAt is non-decreasing
			for i := 1; i < len(records); i++ {
				Expect(records[i].InsertedAt.Before(records[i-1].InsertedAt)).To(BeFalse(),
					"records should be ordered by insertedAt ascending")
			}
		})
	})
})
