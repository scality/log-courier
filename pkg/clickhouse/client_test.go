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
			Expect(helper.Client()).NotTo(BeNil())
		})

		It("should execute simple queries", func() {
			err := helper.Client().Exec(ctx, "SELECT 1")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should query and return results", func() {
			rows, err := helper.Client().Query(ctx, "SELECT 1 AS value")
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = rows.Close() }()

			Expect(rows.Next()).To(BeTrue())
			var value uint8
			err = rows.Scan(&value)
			Expect(err).NotTo(HaveOccurred())
			Expect(value).To(Equal(uint8(1)))
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
			err = helper.Client().QueryRow(ctx, query).Scan(&dbCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(dbCount).To(Equal(uint64(1)))

			// Verify tables exist
			var tableCount uint64
			query = fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name IN ('access_logs_federated', 'offsets_federated')", helper.DatabaseName)
			err = helper.Client().QueryRow(ctx, query).Scan(&tableCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(tableCount).To(Equal(uint64(2)))
		})

		It("should teardown schema successfully", func() {
			err := helper.SetupSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = helper.TeardownSchema(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify database is gone
			var dbCount uint64
			query := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s'", helper.DatabaseName)
			err = helper.Client().QueryRow(ctx, query).Scan(&dbCount)
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

		It("should insert into federated table", func() {
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

			// Verify the log was inserted
			var count uint64
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE bucketName = 'test-bucket'", helper.DatabaseName, clickhouse.TableAccessLogsFederated)
			err = helper.Client().QueryRow(ctx, query).Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(1)))
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
			err := helper.Client().QueryRow(ctx, query).Scan(&count)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(uint64(5)))
		})
	})
})
