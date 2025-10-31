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
			now := time.Now()
			err := om.CommitOffset(ctx, "test-bucket", 0, now)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should allow multiple commits for same bucket", func() {
			t1 := time.Now()
			t2 := t1.Add(1 * time.Hour)

			err := om.CommitOffset(ctx, "test-bucket", 0, t1)
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, t2)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should reject empty bucket name", func() {
			now := time.Now()
			err := om.CommitOffset(ctx, "", 0, now)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket name cannot be empty"))
		})

		It("should reject zero timestamp", func() {
			err := om.CommitOffset(ctx, "test-bucket", 0, time.Time{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("timestamp cannot be zero"))
		})
	})

	Describe("GetOffset", func() {
		It("should return zero time for bucket with no offset", func() {
			offset, err := om.GetOffset(ctx, "nonexistent-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			// ClickHouse returns Unix epoch (1970-01-01) as "zero" for DateTime
			// when max() returns NULL on an empty result set
			Expect(offset.Unix()).To(Equal(int64(0)))
		})

		It("should reject empty bucket name", func() {
			_, err := om.GetOffset(ctx, "", 0)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket name cannot be empty"))
		})

		It("should return committed offset", func() {
			now := time.Now()
			err := om.CommitOffset(ctx, "test-bucket", 0, now)
			Expect(err).NotTo(HaveOccurred())

			err = testutil.WaitForOffset(ctx, om, "test-bucket", 0, now.Unix(), 0)
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.Unix()).To(Equal(now.Unix()))
		})

		It("should return latest offset when multiple commits", func() {
			t1 := time.Now()
			t2 := t1.Add(1 * time.Hour)
			t3 := t2.Add(1 * time.Hour)

			err := om.CommitOffset(ctx, "test-bucket", 0, t1)
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, t2)
			Expect(err).NotTo(HaveOccurred())

			err = om.CommitOffset(ctx, "test-bucket", 0, t3)
			Expect(err).NotTo(HaveOccurred())

			err = testutil.WaitForOffset(ctx, om, "test-bucket", 0, t3.Unix(), 0)
			Expect(err).NotTo(HaveOccurred())

			offset, err := om.GetOffset(ctx, "test-bucket", 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(offset.Unix()).To(Equal(t3.Unix()))
		})
	})
})
