package e2e_test

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket Operations", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs bucket management operations", func() {
		_, err := testCtx.S3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD bucket should succeed")

		_, err = testCtx.S3Client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket location should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.HEAD.BUCKET", 200),
			testCtx.BucketOp("REST.GET.LOCATION", 200),
		)
	})

	It("logs GET bucket versioning operation", func() {
		_, err := testCtx.S3Client.GetBucketVersioning(context.Background(), &s3.GetBucketVersioningInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket versioning should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.GET.VERSIONING", 200),
		)
	})

	It("logs delete bucket operation", func() {
		_, err := testCtx.S3Client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.DELETE.BUCKET", 204),
		)
	})

	It("logs bucket ACL and tagging operations", func() {
		_, err := testCtx.S3Client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket ACL should succeed")

		_, err = testCtx.S3Client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{
			Bucket: aws.String(testCtx.SourceBucket),
			ACL:    types.BucketCannedACLPrivate,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket ACL should succeed")

		_, err = testCtx.S3Client.PutBucketTagging(context.Background(), &s3.PutBucketTaggingInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{
					{Key: aws.String("Environment"), Value: aws.String("test")},
					{Key: aws.String("Team"), Value: aws.String("e2e")},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket tagging should succeed")

		_, err = testCtx.S3Client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket tagging should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.GET.ACL", 200),
			testCtx.BucketOp("REST.PUT.ACL", 200),
			testCtx.BucketOp("REST.PUT.TAGGING", 200),
			testCtx.BucketOp("REST.GET.TAGGING", 200),
		)
	})
})
