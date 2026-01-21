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

		om = logcourier.NewOffsetManager(helper.Client(), helper.DatabaseName)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("CommitOffset", func() {
		It("should commit and retrieve offset", func() {
			insertedAt := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
			timestamp := time.Now().Add(-1 * time.Hour)
			reqID := "test-req-123"

			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{
				InsertedAt: insertedAt,
				StartTime:  timestamp,
				ReqID:      reqID,
			})
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(insertedAt.Unix()))
			Expect(offset.StartTime.Unix()).To(Equal(timestamp.Unix()))
			Expect(offset.ReqID).To(Equal(reqID))
		})

		It("should allow multiple commits for same bucket", func() {
			t1 := time.Now().Truncate(time.Second)
			t2 := t1.Add(1 * time.Hour)

			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: t1, StartTime: t1, ReqID: "req-1"})
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: t2, StartTime: t2, ReqID: "req-2"})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject empty bucket name", func() {
			now := time.Now()
			err := om.CommitOffset(ctx, "", 0, logcourier.Offset{InsertedAt: now, StartTime: now, ReqID: "req-123"})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket name cannot be empty"))
		})

		It("should reject zero insertedAt", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: time.Time{}, StartTime: time.Now(), ReqID: "req-123"})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("insertedAt timestamp cannot be zero"))
		})

		It("should reject zero startTime", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: time.Now(), StartTime: time.Time{}, ReqID: "req-123"})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("startTime cannot be zero"))
		})

		It("should reject empty reqID", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: time.Now(), StartTime: time.Now(), ReqID: ""})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("reqID cannot be empty"))
		})
	})

	Describe("GetOffset", func() {
		It("should return zero values for bucket with no offset", func() {
			offset, err := om.GetOffset(ctx, "nonexistent-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.IsZero()).To(BeTrue())
			Expect(offset.StartTime.IsZero()).To(BeTrue())
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

			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{
				InsertedAt: insertedAt,
				StartTime:  timestamp,
				ReqID:      reqID,
			})
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

			err := om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: t1, StartTime: t1, ReqID: "req-1"})
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: t2, StartTime: t2, ReqID: "req-2"})
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, logcourier.Offset{InsertedAt: t3, StartTime: t3, ReqID: "req-3"})
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.InsertedAt.Unix()).To(Equal(t3.Unix()))
			Expect(offset.ReqID).To(Equal("req-3"))
		})

		It("should return max of all three offset components", func() {
			bucket := "test-bucket"
			baseTime := time.Now().Add(-1 * time.Hour).Truncate(time.Second)

			// Scenario: multiple offsets with different combinations
			// Insert in random order to verify ORDER BY works correctly

			// Offset 1: earliest insertedAt
			ts1 := baseTime.Add(-2 * time.Hour)
			err := om.CommitOffset(ctx, bucket, 0, logcourier.Offset{InsertedAt: ts1, StartTime: ts1, ReqID: "req-aaa"})
			Expect(err).NotTo(HaveOccurred())

			// Offset 2: same insertedAt as offset 3, but earlier timestamp
			ts2 := baseTime
			err = om.CommitOffset(ctx, bucket, 0, logcourier.Offset{InsertedAt: ts2, StartTime: ts2.Add(-1 * time.Second), ReqID: "req-zzz"})
			Expect(err).NotTo(HaveOccurred())

			// Offset 3: same insertedAt as offset 2, later timestamp, but earlier reqID
			err = om.CommitOffset(ctx, bucket, 0, logcourier.Offset{InsertedAt: ts2, StartTime: ts2, ReqID: "req-aaa"})
			Expect(err).NotTo(HaveOccurred())

			// Offset 4: same insertedAt and timestamp as offset 5, but earlier reqID
			err = om.CommitOffset(ctx, bucket, 0, logcourier.Offset{InsertedAt: ts2, StartTime: ts2, ReqID: "req-bbb"})
			Expect(err).NotTo(HaveOccurred())

			// Offset 5: MAXIMUM - same insertedAt and timestamp as offset 4, but later reqID
			err = om.CommitOffset(ctx, bucket, 0, logcourier.Offset{InsertedAt: ts2, StartTime: ts2, ReqID: "req-zzz-max"})
			Expect(err).NotTo(HaveOccurred())

			// GetOffset should return offset 5 (max of all three components)
			offset, err := om.GetOffset(ctx, bucket, 0)
			Expect(err).NotTo(HaveOccurred())

			// Verify it's the maximum by all three components
			Expect(offset.InsertedAt.Unix()).To(Equal(ts2.Unix()))
			Expect(offset.StartTime.Unix()).To(Equal(ts2.Unix()))
			Expect(offset.ReqID).To(Equal("req-zzz-max"))
		})
	})
})
