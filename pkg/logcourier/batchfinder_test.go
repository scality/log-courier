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

		finder = logcourier.NewBatchFinder(helper.Client(), helper.DatabaseName, 5, 60, 3)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("FindBatches", func() {
		Describe("Edge Cases", func() {
			It("should return empty list when no logs", func() {
				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})

			It("should return epoch offset when no offset exists (first processing)", func() {
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "test-bucket",
						StartTime:      time.Now(),
						ReqID:          fmt.Sprintf("req-%d", i),
						Action:         "GetObject",
					})
					Expect(err).NotTo(HaveOccurred())
				}

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0].Bucket).To(Equal("test-bucket"))
				Expect(batches[0].LogCount).To(BeNumerically(">=", 6))

				epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
				Expect(batches[0].LastProcessedOffset.InsertedAt).To(Equal(epoch))
				Expect(batches[0].LastProcessedOffset.ReqID).To(Equal(""))
			})

			It("should not return batches when all logs already processed", func() {
				oldTime := time.Now().Add(-2 * time.Hour)

				for i := 0; i < 6; i++ {
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					err := helper.Client().Exec(ctx, query,
						oldTime.Add(time.Duration(i)*time.Second), // insertedAt
						"test-bucket", // bucketName
						oldTime.Add(time.Duration(i)*time.Second), // startTime
						fmt.Sprintf("req-%03d", i),                // req_id
						"GetObject",                               // operation
						true,                                      // loggingEnabled
						uint16(0),                                 // raftSessionID
						"/test-bucket/key",                        // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set offset to last log
				lastLogTime := oldTime.Add(5 * time.Second)
				offsetQuery := fmt.Sprintf(
					"INSERT INTO %s.%s (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)",
					helper.DatabaseName, clickhouse.TableOffsetsFederated)
				err := helper.Client().Exec(ctx, offsetQuery,
					"test-bucket", uint16(0), lastLogTime, lastLogTime, "req-007")
				Expect(err).NotTo(HaveOccurred())

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})
		})

		Describe("Threshold Conditions", func() {
			It("should return log batch when time threshold met", func() {
				// Insert 1 log directly into access_logs with old insertedAt
				// This bypasses the materialized view to set insertedAt manually
				oldTime := time.Now().Add(-2 * time.Hour)
				query := fmt.Sprintf(`
					INSERT INTO %s.%s
					(bucketOwner, bucketName, startTime, clientIP, requester, req_id, operation,
					 objectKey, requestURI, httpCode, errorCode, bytesSent, objectSize, totalTime,
					 turnAroundTime, referer, userAgent, versionId, signatureVersion, cipherSuite,
					 authenticationType, hostHeader, tlsVersion, aclRequired, insertedAt,
					 loggingTargetBucket, loggingTargetPrefix, raftSessionID)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
				err := helper.Client().Exec(ctx, query,
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

			It("should return batch when both thresholds met", func() {
				oldTime := time.Now().Add(-2 * time.Hour)

				for i := 0; i < 6; i++ {
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					err := helper.Client().Exec(ctx, query,
						oldTime,                    // insertedAt (old)
						"test-bucket",              // bucketName
						oldTime,                    // startTime
						fmt.Sprintf("req-%03d", i), // req_id
						"GetObject",                // operation
						true,                       // loggingEnabled
						uint16(0),                  // raftSessionID
						"/test-bucket/key",         // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(1))
				Expect(batches[0].Bucket).To(Equal("test-bucket"))
				Expect(batches[0].LogCount).To(BeNumerically(">=", 6))
			})

			It("should not return batches when neither threshold met", func() {
				for i := 0; i < 3; i++ {
					err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "test-bucket",
						StartTime:      time.Now(),
						ReqID:          fmt.Sprintf("req-%d", i),
						Action:         "GetObject",
					})
					Expect(err).NotTo(HaveOccurred())
				}

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(BeEmpty())
			})
		})

		Describe("Composite Offset Filtering", func() {
			It("should filter when offset differs in insertedAt", func() {
				baseTime := time.Now().Add(-2 * time.Hour)
				startTime := baseTime
				reqID := "req-1"

				// Insert 6 logs with insertedAt from T to T+5s, same startTime and reqID
				for i := 0; i < 6; i++ {
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					err := helper.Client().Exec(ctx, query,
						baseTime.Add(time.Duration(i)*time.Second), // insertedAt varies
						"test-bucket",      // bucketName
						startTime,          // startTime (same for all)
						reqID,              // req_id (same for all)
						"GetObject",        // operation
						true,               // loggingEnabled
						uint16(0),          // raftSessionID
						"/test-bucket/key", // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set offset to insertedAt=T+3s
				offsetTime := baseTime.Add(3 * time.Second)
				offsetQuery := fmt.Sprintf(
					"INSERT INTO %s.%s (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)",
					helper.DatabaseName, clickhouse.TableOffsetsFederated)
				err := helper.Client().Exec(ctx, offsetQuery,
					"test-bucket", uint16(0), offsetTime, startTime, reqID)
				Expect(err).NotTo(HaveOccurred())

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Should find logs with insertedAt > T+3s (i.e., T+4s, T+5s = 2 logs)
				Expect(batches).To(HaveLen(1))
				Expect(batches[0].LogCount).To(Equal(uint64(2)))
			})

			It("should filter when offset differs in startTime (same insertedAt)", func() {
				baseTime := time.Now().Add(-2 * time.Hour)
				insertedAt := baseTime

				// Insert 6 logs with same insertedAt, varying startTime
				for i := 0; i < 6; i++ {
					startTime := baseTime.Add(time.Duration(i) * time.Second)
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					err := helper.Client().Exec(ctx, query,
						insertedAt,                 // insertedAt (same for all)
						"test-bucket",              // bucketName
						startTime,                  // startTime varies
						fmt.Sprintf("req-%03d", i), // req_id
						"GetObject",                // operation
						true,                       // loggingEnabled
						uint16(0),                  // raftSessionID
						"/test-bucket/key",         // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				// Set offset to (insertedAt=T, startTime=T+3s, reqID="req-003")
				offsetTimestamp := baseTime.Add(3 * time.Second)
				offsetQuery := fmt.Sprintf(
					"INSERT INTO %s.%s (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)",
					helper.DatabaseName, clickhouse.TableOffsetsFederated)
				err := helper.Client().Exec(ctx, offsetQuery,
					"test-bucket", uint16(0), insertedAt, offsetTimestamp, "req-003")
				Expect(err).NotTo(HaveOccurred())

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Should find logs with startTime > T+3s (i.e., T+4s, T+5s = 2 logs)
				Expect(batches).To(HaveLen(1))
				Expect(batches[0].LogCount).To(Equal(uint64(2)))
			})

			It("should use most recent offset when multiple offsets exist", func() {
				baseTime := time.Now().Add(-2 * time.Hour)

				// Insert logs at T0, T1, T2, T3
				for i := 0; i < 4; i++ {
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					insertedAt := baseTime.Add(time.Duration(i) * time.Second)
					err := helper.Client().Exec(ctx, query,
						insertedAt,                 // insertedAt
						"test-bucket",              // bucketName
						insertedAt,                 // startTime
						fmt.Sprintf("req-%03d", i), // req_id
						"GetObject",                // operation
						true,                       // loggingEnabled
						uint16(0),                  // raftSessionID
						"/test-bucket/key",         // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				// Insert 3 offset records: T0, T2, T1 (in this order to test ROW_NUMBER)
				offsetQuery := fmt.Sprintf(
					"INSERT INTO %s.%s (bucketName, raftSessionID, lastProcessedInsertedAt, lastProcessedStartTime, lastProcessedReqId) VALUES (?, ?, ?, ?, ?)",
					helper.DatabaseName, clickhouse.TableOffsetsFederated)

				t0 := baseTime
				err := helper.Client().Exec(ctx, offsetQuery,
					"test-bucket", uint16(0), t0, t0, "req-000")
				Expect(err).NotTo(HaveOccurred())

				t2 := baseTime.Add(2 * time.Second)
				err = helper.Client().Exec(ctx, offsetQuery,
					"test-bucket", uint16(0), t2, t2, "req-002")
				Expect(err).NotTo(HaveOccurred())

				t1 := baseTime.Add(1 * time.Second)
				err = helper.Client().Exec(ctx, offsetQuery,
					"test-bucket", uint16(0), t1, t1, "req-001")
				Expect(err).NotTo(HaveOccurred())

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())

				// ROW_NUMBER should pick T2 (highest), so only log at T3 should be found
				Expect(batches).To(HaveLen(1))
				Expect(batches[0].LogCount).To(Equal(uint64(1)))
				Expect(batches[0].LastProcessedOffset.InsertedAt.Unix()).To(Equal(t2.Unix()))
				Expect(batches[0].LastProcessedOffset.ReqID).To(Equal("req-002"))
			})
		})

		Describe("Multiple Buckets", func() {
			It("should handle multiple buckets", func() {
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "bucket-1",
						StartTime:      time.Now(),
						ReqID:          fmt.Sprintf("req-1-%d", i),
						Action:         "GetObject",
					})
					Expect(err).NotTo(HaveOccurred())
				}

				for i := 0; i < 6; i++ {
					err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "bucket-2",
						StartTime:      time.Now(),
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

			It("should order multiple batches by oldest first", func() {
				oldTime := time.Now().Add(-2 * time.Hour)
				newerTime := time.Now().Add(-1 * time.Hour)

				// Insert logs for bucket-A with older insertedAt
				for i := 0; i < 6; i++ {
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					err := helper.Client().Exec(ctx, query,
						oldTime,                      // insertedAt (older)
						"bucket-A",                   // bucketName
						oldTime,                      // startTime
						fmt.Sprintf("req-a-%03d", i), // req_id
						"GetObject",                  // operation
						true,                         // loggingEnabled
						uint16(0),                    // raftSessionID
						"/bucket-A/key",              // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				// Insert logs for bucket-B with newer insertedAt
				for i := 0; i < 6; i++ {
					query := fmt.Sprintf(`
						INSERT INTO %s.%s
						(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
						VALUES (?, ?, ?, ?, ?, ?, ?, ?)
					`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
					err := helper.Client().Exec(ctx, query,
						newerTime,                    // insertedAt (newer)
						"bucket-B",                   // bucketName
						newerTime,                    // startTime
						fmt.Sprintf("req-b-%03d", i), // req_id
						"GetObject",                  // operation
						true,                         // loggingEnabled
						uint16(0),                    // raftSessionID
						"/bucket-B/key",              // requestURI
					)
					Expect(err).NotTo(HaveOccurred())
				}

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(2))

				// bucket-A should come first
				Expect(batches[0].Bucket).To(Equal("bucket-A"))
				Expect(batches[1].Bucket).To(Equal("bucket-B"))
			})
		})

		Describe("RaftSessionID Separation", func() {
			It("should return separate batches for same bucket with different raftSessionIDs", func() {
				// Insert logs for test-bucket with raftSessionID=1
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "test-bucket",
						RaftSessionID:  1,
						StartTime:      time.Now(),
						ReqID:          fmt.Sprintf("req-1-%d", i),
						Action:         "GetObject",
					})
					Expect(err).NotTo(HaveOccurred())
				}

				// Insert logs for test-bucket with raftSessionID=2
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "test-bucket",
						RaftSessionID:  2,
						StartTime:      time.Now(),
						ReqID:          fmt.Sprintf("req-2-%d", i),
						Action:         "GetObject",
					})
					Expect(err).NotTo(HaveOccurred())
				}

				batches, err := finder.FindBatches(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batches).To(HaveLen(2))

				// Both batches should be for test-bucket but different raftSessionIDs
				Expect(batches[0].Bucket).To(Equal("test-bucket"))
				Expect(batches[1].Bucket).To(Equal("test-bucket"))

				raftSessionIDs := []uint16{batches[0].RaftSessionID, batches[1].RaftSessionID}
				Expect(raftSessionIDs).To(ConsistOf(uint16(1), uint16(2)))
			})
		})

		It("should not reprocess logs when insertedAt order differs from startTime order", func() {
			// - Log A arrives quickly (insertedAt=10:00:03, startTime=10:00:02)
			// - Log B arrives delayed (insertedAt=10:00:05, startTime=10:00:01)
			// - Next cycle should not re-discover log B

			baseTime := time.Now().Add(-1 * time.Hour)

			// Insert Log A: operation at :02, arrives at :03
			logAStartTime := baseTime.Add(2 * time.Second)
			logAInsertedAt := baseTime.Add(3 * time.Second)
			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)
			err := helper.Client().Exec(ctx, query,
				logAInsertedAt, "test-bucket", logAStartTime, "req-A",
				"GetObject", true, uint16(0), "/test-bucket/key-a")
			Expect(err).NotTo(HaveOccurred())

			// Insert Log B: operation at :01, arrives at :05
			logBStartTime := baseTime.Add(1 * time.Second)
			logBInsertedAt := baseTime.Add(5 * time.Second)
			err = helper.Client().Exec(ctx, query,
				logBInsertedAt, "test-bucket", logBStartTime, "req-B",
				"GetObject", true, uint16(0), "/test-bucket/key-b")
			Expect(err).NotTo(HaveOccurred())

			// Cycle 1: Find batches
			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(HaveLen(1))
			Expect(batches[0].LogCount).To(BeNumerically(">=", 2))

			// Cycle 1: Fetch logs and simulate processing
			fetcher := logcourier.NewLogFetcher(helper.Client(), helper.DatabaseName, 10000)
			records, err := fetcher.FetchLogs(ctx, batches[0])
			Expect(err).NotTo(HaveOccurred())
			Expect(records).To(HaveLen(2))

			// Cycle 1: Commit offset from last fetched log
			// (simulating what Processor does)
			lastLog := records[len(records)-1]
			offsetMgr := logcourier.NewOffsetManager(helper.Client(), helper.DatabaseName)
			err = offsetMgr.CommitOffset(ctx, "test-bucket", uint16(0), logcourier.Offset{
				InsertedAt: lastLog.InsertedAt,
				StartTime:  lastLog.StartTime,
				ReqID:      lastLog.ReqID,
			})
			Expect(err).NotTo(HaveOccurred())

			// Cycle 2: Find batches again
			batches, err = finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(batches).To(BeEmpty(), "Expected no batches after processing all logs, but found %d batch(es)", len(batches))
		})

		It("should sort batches by oldest min_ts first", func() {
			recentTime := time.Now()
			for i := 0; i < 5; i++ {
				err := helper.InsertTestLog(ctx, testutil.TestLogRecord{
					LoggingEnabled: true,
					BucketName:     "bucket-recent",
					StartTime:      recentTime,
					ReqID:          fmt.Sprintf("req-recent-%d", i),
					Action:         "GetObject",
				})
				Expect(err).NotTo(HaveOccurred())
			}

			// Insert logs for bucket-old with old startTime
			oldTime := time.Now().Add(-2 * time.Hour)
			query := fmt.Sprintf(`
				INSERT INTO %s.%s
				(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

			for i := 0; i < 5; i++ {
				err := helper.Client().Exec(ctx, query,
					oldTime,
					"bucket-old",
					oldTime,
					fmt.Sprintf("req-old-%d", i),
					"GetObject",
					true,
					uint16(0),
					"/bucket-old/key",
				)
				Expect(err).NotTo(HaveOccurred())
			}

			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(batches).To(HaveLen(2))

			// First batch should be the older one
			Expect(batches[0].Bucket).To(Equal("bucket-old"))
			Expect(batches[1].Bucket).To(Equal("bucket-recent"))
		})

		It("should limit number of buckets returned", func() {
			for bucketNum := 0; bucketNum < 5; bucketNum++ {
				bucketName := fmt.Sprintf("bucket-%d", bucketNum)
				insertTime := time.Now().Add(-time.Duration(4-bucketNum) * time.Hour)

				query := fmt.Sprintf(`
					INSERT INTO %s.%s
					(insertedAt, bucketName, startTime, req_id, operation, loggingEnabled, raftSessionID, requestURI)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?)
				`, helper.DatabaseName, clickhouse.TableAccessLogsFederated)

				for i := 0; i < 5; i++ {
					err := helper.Client().Exec(ctx, query,
						insertTime,
						bucketName,
						insertTime,
						fmt.Sprintf("req-%d-%d", bucketNum, i),
						"GetObject",
						true,
						uint16(0),
						fmt.Sprintf("/%s/key", bucketName),
					)
					Expect(err).NotTo(HaveOccurred())
				}
			}

			batches, err := finder.FindBatches(ctx)
			Expect(err).NotTo(HaveOccurred())

			Expect(batches).To(HaveLen(3))
			bucketNames := []string{batches[0].Bucket, batches[1].Bucket, batches[2].Bucket}
			Expect(bucketNames).To(Equal([]string{"bucket-0", "bucket-1", "bucket-2"}))
		})
	})
})
