package e2e_test

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket Operations", func() {
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("logs bucket management operations", func() {
		// HEAD Bucket
		By("performing head bucket")
		_, err := ctx.S3Client.HeadBucket(context.Background(), &s3.HeadBucketInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD bucket should succeed")

		// GET Bucket Location
		By("getting bucket location")
		_, err = ctx.S3Client.GetBucketLocation(context.Background(), &s3.GetBucketLocationInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket location should succeed")

		// Wait for logs (2 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 2)

		// Verify operations
		By("verifying head bucket log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.HEAD.BUCKET",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying get bucket location log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.LOCATION",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("logs bucket versioning operations", func() {
		testKey := "versioned-object.txt"

		// Enable versioning on source bucket
		By("enabling versioning on source bucket")
		_, err := ctx.S3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
			Bucket: aws.String(ctx.SourceBucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		Expect(err).NotTo(HaveOccurred(), "Enable versioning should succeed")

		// PUT Object version
		By("putting version of object")
		putV1Resp, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader([]byte("version content")),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT object version should succeed")
		versionID := putV1Resp.VersionId

		// List Object Versions
		By("listing object versions")
		_, err = ctx.S3Client.ListObjectVersions(context.Background(), &s3.ListObjectVersionsInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "List object versions should succeed")

		// DELETE Object (creates delete marker)
		By("deleting object (creates delete marker)")
		deleteResp, err := ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE object should succeed")
		deleteMarkerVersionID := deleteResp.VersionId

		// GET Object Version (specific version)
		By("getting specific object version")
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: versionID,
		})
		Expect(err).NotTo(HaveOccurred(), "GET object version should succeed")

		// DELETE Object Version (delete the delete marker)
		By("deleting delete marker version")
		_, err = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: deleteMarkerVersionID,
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE object version should succeed")

		// Wait for logs (6 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 6)

		// Verify operations
		By("verifying put bucket versioning log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.VERSIONING",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying put object version log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.PUT.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying put object has VersionID when versioning enabled")
		Expect(logs[1].VersionID).NotTo(BeEmpty(),
			"VersionID should be present for PUT with versioning enabled")

		By("verifying list object versions log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.GET.BUCKET",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying delete object (create delete marker) log")
		verifyLogRecord(logs[3], ExpectedLog{
			Operation:  "REST.DELETE.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 204,
		})

		By("verifying get object version log")
		verifyLogRecord(logs[4], ExpectedLog{
			Operation:  "REST.GET.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying get object version has VersionID")
		Expect(logs[4].VersionID).NotTo(BeEmpty(),
			"VersionID should be present for GET with versionId parameter")

		By("verifying delete object version log")
		verifyLogRecord(logs[5], ExpectedLog{
			Operation:  "REST.DELETE.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)

		// Cleanup: Delete remaining version
		By("cleaning up remaining version")
		_, _ = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: versionID,
		})
	})

	It("logs bucket ACL and tagging operations", func() {
		// GET Bucket ACL
		By("getting bucket ACL")
		_, err := ctx.S3Client.GetBucketAcl(context.Background(), &s3.GetBucketAclInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket ACL should succeed")

		// PUT Bucket ACL
		By("putting bucket ACL")
		_, err = ctx.S3Client.PutBucketAcl(context.Background(), &s3.PutBucketAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			ACL:    types.BucketCannedACLPrivate,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket ACL should succeed")

		// PUT Bucket Tagging
		By("putting bucket tagging")
		_, err = ctx.S3Client.PutBucketTagging(context.Background(), &s3.PutBucketTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{
					{Key: aws.String("Environment"), Value: aws.String("test")},
					{Key: aws.String("Team"), Value: aws.String("e2e")},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket tagging should succeed")

		// GET Bucket Tagging
		By("getting bucket tagging")
		_, err = ctx.S3Client.GetBucketTagging(context.Background(), &s3.GetBucketTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket tagging should succeed")

		// Wait for logs (4 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 4)

		// Verify operations
		By("verifying get bucket ACL log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.GET.ACL",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying put bucket ACL log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.PUT.ACL",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying put bucket tagging log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.PUT.TAGGING",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying get bucket tagging log")
		verifyLogRecord(logs[3], ExpectedLog{
			Operation:  "REST.GET.TAGGING",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})
})
