package clickhouse_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("ClickHouse Client", func() {
	var (
		ctx    context.Context
		helper *testutil.ClickHouseTestHelper
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		helper, err = testutil.NewClickHouseTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(helper).NotTo(BeNil())
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.Close()
		}
	})

	Describe("Connection", func() {
		It("should connect successfully", func() {
			Expect(helper.Client).NotTo(BeNil())
		})

		It("should execute simple queries", func() {
			err := helper.Client.Exec(ctx, "SELECT 1")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should query and return results", func() {
			rows, err := helper.Client.Query(ctx, "SELECT 1 AS value")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = rows.Close() }()

			Expect(rows.Next()).To(BeTrue())
			var value uint8
			err = rows.Scan(&value)
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(uint8(1)))
		})
	})

	Describe("Schema Management", func() {
		AfterEach(func() {
			// Clean up schema after each test
			_ = helper.TeardownSchema(ctx)
		})

		It("should create schema successfully", func() {
			err := helper.SetupSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify database exists
			var dbCount uint64
			err = helper.Client.QueryRow(ctx, "SELECT count() FROM system.databases WHERE name = 'logs'").Scan(&dbCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(dbCount).To(Equal(uint64(1)))

			// Verify tables exist
			var tableCount uint64
			err = helper.Client.QueryRow(ctx,
				"SELECT count() FROM system.tables WHERE database = 'logs' AND name IN ('access_logs_ingest', 'access_logs', 'offsets')").Scan(&tableCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(tableCount).To(Equal(uint64(3)))

			// Verify materialized view exists
			var mvCount uint64
			err = helper.Client.QueryRow(ctx,
				"SELECT count() FROM system.tables WHERE database = 'logs' AND name = 'access_logs_ingest_mv'").Scan(&mvCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(mvCount).To(Equal(uint64(1)))
		})

		It("should teardown schema successfully", func() {
			err := helper.SetupSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = helper.TeardownSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify tables are gone
			var tableCount uint64
			err = helper.Client.QueryRow(ctx,
				"SELECT count() FROM system.tables WHERE database = 'logs' AND name IN ('access_logs_ingest', 'access_logs', 'access_logs_ingest_mv', 'offsets')").Scan(&tableCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(tableCount).To(Equal(uint64(0)))
		})
	})

	Describe("Data Operations", func() {
		BeforeEach(func() {
			err := helper.SetupSchema(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			_ = helper.TeardownSchema(ctx)
		})

		It("should insert into ingest table", func() {
			testLog := testutil.TestLogRecord{
				Timestamp:      time.Now(),
				BucketName:     "test-bucket",
				ReqID:          "req-123",
				Action:         "GetObject",
				LoggingEnabled: true,
				RaftSessionID:  1,
				HttpCode:       200,
				BytesSent:      1024,
			}

			err := helper.InsertTestLog(ctx, testLog)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should filter logs through materialized view when loggingEnabled=true", func() {
			now := time.Now()

			// Insert log with loggingEnabled = true
			testLog1 := testutil.TestLogRecord{
				Timestamp:      now,
				BucketName:     "test-bucket-enabled",
				ReqID:          "req-enabled",
				Action:         "GetObject",
				LoggingEnabled: true,
				RaftSessionID:  1,
				HttpCode:       200,
				BytesSent:      1024,
			}
			err := helper.InsertTestLog(ctx, testLog1)
			Expect(err).NotTo(HaveOccurred())

			// Insert log with loggingEnabled = false
			testLog2 := testutil.TestLogRecord{
				Timestamp:      now,
				BucketName:     "test-bucket-disabled",
				ReqID:          "req-disabled",
				Action:         "PutObject",
				LoggingEnabled: false,
				RaftSessionID:  1,
				HttpCode:       201,
				BytesSent:      2048,
			}
			err = helper.InsertTestLog(ctx, testLog2)
			Expect(err).NotTo(HaveOccurred())

			// Wait for materialized view to process
			err = helper.WaitForMaterializedView(ctx,
				"SELECT COUNT(*) FROM logs.access_logs WHERE req_id = 'req-enabled'",
				5*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Verify only loggingEnabled=true record is in access_logs table
			var count uint64
			err = helper.Client.QueryRow(ctx,
				"SELECT COUNT(*) FROM logs.access_logs WHERE bucketName IN ('test-bucket-enabled', 'test-bucket-disabled')").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(1)))

			// Verify it's the correct record
			var reqID string
			err = helper.Client.QueryRow(ctx,
				"SELECT req_id FROM logs.access_logs WHERE bucketName = 'test-bucket-enabled'").Scan(&reqID)
			Expect(err).NotTo(HaveOccurred())
			Expect(reqID).To(Equal("req-enabled"))

			// Verify disabled record is NOT in access_logs
			var disabledCount uint64
			err = helper.Client.QueryRow(ctx,
				"SELECT COUNT(*) FROM logs.access_logs WHERE bucketName = 'test-bucket-disabled'").Scan(&disabledCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(disabledCount).To(Equal(uint64(0)))
		})

		It("should handle multiple inserts", func() {
			now := time.Now()

			// Insert multiple logs
			for i := 0; i < 5; i++ {
				testLog := testutil.TestLogRecord{
					Timestamp:      now.Add(time.Duration(i) * time.Second),
					BucketName:     "test-bucket-multi",
					ReqID:          fmt.Sprintf("req-%d", i),
					Action:         "GetObject",
					LoggingEnabled: true,
					RaftSessionID:  1,
					HttpCode:       200,
					BytesSent:      uint64(1024 * (i + 1)),
				}
				err := helper.InsertTestLog(ctx, testLog)
				Expect(err).NotTo(HaveOccurred())
			}

			// Wait for materialized view to process all records
			err := helper.WaitForMaterializedView(ctx,
				"SELECT COUNT(*) FROM logs.access_logs WHERE bucketName = 'test-bucket-multi'",
				5*time.Second)
			Expect(err).NotTo(HaveOccurred())

			// Verify all 5 records are present
			var count uint64
			err = helper.Client.QueryRow(ctx,
				"SELECT COUNT(*) FROM logs.access_logs WHERE bucketName = 'test-bucket-multi'").Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})
	})
})
