package clickhouse_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/clickhouse"
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

		It("should accept multiple hosts", func() {
			cfg := clickhouse.Config{
				Hosts:    []string{"localhost:9002", "localhost:9003"},
				Username: "default",
				Password: "",
				Timeout:  10 * time.Second,
			}

			client, err := clickhouse.NewClient(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = client.Close() }()

			Expect(client).NotTo(BeNil())
		})

		It("should reject empty host list", func() {
			cfg := clickhouse.Config{
				Hosts:    []string{},
				Username: "default",
				Password: "",
				Timeout:  10 * time.Second,
			}

			_, err := clickhouse.NewClient(ctx, cfg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least one host"))
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
			query := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s'", helper.DatabaseName)
			err = helper.Client.QueryRow(ctx, query).Scan(&dbCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(dbCount).To(Equal(uint64(1)))

			// Verify tables exist
			var tableCount uint64
			query = fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name IN ('access_logs_ingest', 'access_logs_federated', 'offsets_federated')", helper.DatabaseName)
			err = helper.Client.QueryRow(ctx, query).Scan(&tableCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(tableCount).To(Equal(uint64(3)))

			// Verify materialized view exists
			var mvCount uint64
			query = fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = 'access_logs_ingest_mv'", helper.DatabaseName)
			err = helper.Client.QueryRow(ctx, query).Scan(&mvCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(mvCount).To(Equal(uint64(1)))
		})

		It("should teardown schema successfully", func() {
			err := helper.SetupSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = helper.TeardownSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify database is gone
			var dbCount uint64
			query := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s'", helper.DatabaseName)
			err = helper.Client.QueryRow(ctx, query).Scan(&dbCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(dbCount).To(Equal(uint64(0)))
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

			// Verify only loggingEnabled=true record is in access_logs table
			var count uint64
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE bucketName IN ('test-bucket-enabled', 'test-bucket-disabled')", helper.DatabaseName, clickhouse.TableAccessLogsFederated)
			err = helper.Client.QueryRow(ctx, query).Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(1)))

			// Verify it's the correct record
			var reqID string
			query = fmt.Sprintf("SELECT req_id FROM %s.%s WHERE bucketName = 'test-bucket-enabled'", helper.DatabaseName, clickhouse.TableAccessLogsFederated)
			err = helper.Client.QueryRow(ctx, query).Scan(&reqID)
			Expect(err).NotTo(HaveOccurred())
			Expect(reqID).To(Equal("req-enabled"))

			// Verify disabled record is NOT in access_logs
			var disabledCount uint64
			query = fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE bucketName = 'test-bucket-disabled'", helper.DatabaseName, clickhouse.TableAccessLogsFederated)
			err = helper.Client.QueryRow(ctx, query).Scan(&disabledCount)
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

			// Verify all 5 records are present
			var count uint64
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE bucketName = 'test-bucket-multi'", helper.DatabaseName, clickhouse.TableAccessLogsFederated)
			err := helper.Client.QueryRow(ctx, query).Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})
	})
})
