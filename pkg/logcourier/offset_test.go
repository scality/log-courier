package logcourier_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/testutil"
)

var _ = Describe("OffsetManager", func() {
	var (
		ctx    context.Context
		helper *testutil.ClickHouseTestHelper
		om     *logcourier.OffsetManager
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		helper, err = testutil.NewClickHouseTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = helper.SetupSchema(ctx)
		Expect(err).NotTo(HaveOccurred())

		om = logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("CommitOffset", func() {
		It("should commit offset successfully", func() {
			insertedAt := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
			timestamp := time.Now().Add(-1 * time.Hour)
			reqID := "test-req-1"

			err := om.CommitOffset(ctx, "test-bucket", 0, insertedAt, timestamp, reqID)
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(insertedAt.Unix()))
			Expect(offset.Timestamp.Unix()).To(Equal(timestamp.Unix()))
			Expect(offset.ReqID).To(Equal(reqID))
		})

		It("should commit and retrieve triple composite offset", func() {
			insertedAt := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
			timestamp := time.Now().Add(-1 * time.Hour)
			reqID := "test-req-123"

			err := om.CommitOffset(ctx, "test-bucket", 0, insertedAt, timestamp, reqID)
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(insertedAt.Unix()))
			// ClickHouse DateTime64 through sql.NullTime may truncate to second precision
			Expect(offset.Timestamp.Unix()).To(Equal(timestamp.Unix()))
			Expect(offset.ReqID).To(Equal(reqID))
		})

		It("should allow multiple commits for same bucket", func() {
			t1 := time.Now().Truncate(time.Second)
			t2 := t1.Add(1 * time.Hour)

			err := om.CommitOffset(ctx, "test-bucket", 0, t1, t1, "req-1")
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, t2, t2, "req-2")
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject empty bucket name", func() {
			now := time.Now()
			err := om.CommitOffset(ctx, "", 0, now, now, "req-123")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket name cannot be empty"))
		})

		It("should reject zero insertedAt", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, time.Time{}, time.Now(), "req-123")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("insertedAt timestamp cannot be zero"))
		})

		It("should reject zero timestamp", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, time.Now(), time.Time{}, "req-123")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("timestamp cannot be zero"))
		})

		It("should reject empty reqID", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, time.Now(), time.Now(), "")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reqID cannot be empty"))
		})
	})

	Describe("GetOffset", func() {
		It("should return zero values for bucket with no offset", func() {
			offset, err := om.GetOffset(ctx, "nonexistent-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.IsZero()).To(BeTrue())
			Expect(offset.Timestamp.IsZero()).To(BeTrue())
			Expect(offset.ReqID).To(Equal(""))
		})

		It("should reject empty bucket name", func() {
			_, err := om.GetOffset(ctx, "", 0)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket name cannot be empty"))
		})

		It("should return committed offset", func() {
			insertedAt := time.Now().Truncate(time.Second)
			timestamp := time.Now()
			reqID := "test-req-1"

			err := om.CommitOffset(ctx, "test-bucket", 0, insertedAt, timestamp, reqID)
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(insertedAt.Unix()))
			Expect(offset.ReqID).To(Equal(reqID))
		})

		It("should return latest offset when multiple commits", func() {
			t1 := time.Now().Truncate(time.Second)
			t2 := t1.Add(1 * time.Hour)
			t3 := t2.Add(1 * time.Hour)

			err := om.CommitOffset(ctx, "test-bucket", 0, t1, t1, "req-1")
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, t2, t2, "req-2")
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, t3, t3, "req-3")
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(t3.Unix()))
			Expect(offset.ReqID).To(Equal("req-3"))
		})

		It("should handle multiple offsets and return latest", func() {
			bucket := "test-bucket"

			// Commit first offset
			ts1 := time.Now().Add(-2 * time.Hour)
			err := om.CommitOffset(ctx, bucket, 0, ts1.Truncate(time.Second), ts1, "req-1")
			Expect(err).NotTo(HaveOccurred())

			// Commit second offset (later)
			ts2 := time.Now().Add(-1 * time.Hour)
			err = om.CommitOffset(ctx, bucket, 0, ts2.Truncate(time.Second), ts2, "req-2")
			Expect(err).NotTo(HaveOccurred())

			// Should get the latest
			offset, err := om.GetOffset(ctx, bucket, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(ts2.Truncate(time.Second).Unix()))
			Expect(offset.ReqID).To(Equal("req-2"))
		})

		It("should handle millisecond precision timestamps", func() {
			now := time.Now()
			insertedAt := now.Truncate(time.Second)
			timestamp := now // Keep millisecond precision
			reqID := "test-req-ms"

			err := om.CommitOffset(ctx, "test-bucket", 0, insertedAt, timestamp, reqID)
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())

			// Verify millisecond precision is preserved (within 1ms tolerance)
			diff := offset.Timestamp.Sub(timestamp)
			Expect(diff).To(BeNumerically("<", time.Millisecond))
		})

		It("should handle logs with same insertedAt and timestamp", func() {
			insertedAt := time.Now().Truncate(time.Second)
			timestamp := time.Now()

			// Commit offset with req-1
			err := om.CommitOffset(ctx, "test-bucket", 0, insertedAt, timestamp, "req-1")
			Expect(err).NotTo(HaveOccurred())

			// Commit offset with req-2 (same times, different reqID)
			err = om.CommitOffset(ctx, "test-bucket", 0, insertedAt, timestamp, "req-2")
			Expect(err).NotTo(HaveOccurred())

			// Should get the latest by insertedAt (which is same, so last written)
			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.ReqID).To(Equal("req-2"))
		})
	})
})
