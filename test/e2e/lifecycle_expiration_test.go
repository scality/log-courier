package e2e_test

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	opExpireObject = "S3.EXPIRE.OBJECT"

	// lifecycleExpirationTimeout is how long to wait for backbeat to expire an object.
	lifecycleExpirationTimeout = 60 * time.Second
	lifecycleExpirationPoll    = 5 * time.Second
)

var _ = Describe("Lifecycle expiration in access logs", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs lifecycle expiration events as S3.EXPIRE.OBJECT", func(ctx context.Context) {
		testKey := "lifecycle-expire-test.txt"
		testContent := []byte("data to be expired by lifecycle")

		// PUT an object that will be expired
		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		// Configure lifecycle expiration with Days: 1.
		// Backbeat's EXPIRE_ONE_DAY_EARLIER flag makes this expire immediately.
		_, err = testCtx.S3Client.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(testCtx.SourceBucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{
					{
						ID:     aws.String("expire-all"),
						Status: types.ExpirationStatusEnabled,
						Filter: &types.LifecycleRuleFilter{Prefix: aws.String("")},
						Expiration: &types.LifecycleExpiration{
							Days: aws.Int32(1),
						},
					},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PutBucketLifecycleConfiguration should succeed")

		// Wait for backbeat to expire the object
		Eventually(func() bool {
			_, err := testCtx.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(testCtx.SourceBucket),
				Key:    aws.String(testKey),
			})
			return err != nil
		}).WithTimeout(lifecycleExpirationTimeout).
			WithPolling(lifecycleExpirationPoll).
			Should(BeTrue(), "Object should be expired by lifecycle")

		// Poll until an S3.EXPIRE.OBJECT log entry appears.
		// We can't use VerifyLogs here because the HeadObject polling above
		// generates REST.HEAD.OBJECT entries that interleave with the logs
		// we care about.
		var expireLog *ParsedLogRecord
		Eventually(func() bool {
			logs, err := fetchAllLogsSince(testCtx, testCtx.TestStartTime)
			if err != nil {
				return false
			}
			for _, log := range logs {
				if log.Operation == opExpireObject && log.Key == testKey {
					expireLog = log
					return true
				}
			}
			return false
		}).WithTimeout(logWaitTimeout).
			WithPolling(logPollInterval).
			Should(BeTrue(), "S3.EXPIRE.OBJECT log should appear")

		// verifyLogRecord handles asserting request-scoped fields are absent
		// for S3.EXPIRE.OBJECT, so we only need lifecycle-specific checks here.
		verifyLogRecord(expireLog, testCtx.ObjectOp(opExpireObject, testKey, 0).
			WithObjectSize(int64(len(testContent))))

		By("verifying requester is the lifecycle service")
		Expect(expireLog.Requester).To(Equal("ScalityS3LifecycleService"))
	})
})
