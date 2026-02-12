package e2e_test

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Error Cases", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs error responses (404s)", func(ctx context.Context) {
		nonExistentKey := "does-not-exist.txt"
		testKey := "error-test-object.txt"
		testContent := []byte("test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT test object should succeed")

		// GET non-existent object (404)
		_, err = testCtx.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		Expect(err).To(HaveOccurred(), "GET non-existent object should fail")

		// DELETE non-existent object (404)
		_, _ = testCtx.S3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		// Note: DELETE on non-existent objects typically returns 204, not 404
		// So we don't check for error here

		// HEAD non-existent object (404)
		_, err = testCtx.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		Expect(err).To(HaveOccurred(), "HEAD non-existent object should fail")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			testCtx.ObjectOp("REST.GET.OBJECT", nonExistentKey, 404).WithErrorCode("NoSuchKey"),
			testCtx.ObjectOp("REST.DELETE.OBJECT", nonExistentKey, 204),
			testCtx.ObjectOp("REST.HEAD.OBJECT", nonExistentKey, 404).WithErrorCode("NoSuchKey"),
		)
	})

	It("logs BucketNotEmpty error when deleting non-empty bucket", func(ctx context.Context) {
		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String("block-delete.txt"),
			Body:   bytes.NewReader([]byte("data")),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT object should succeed")

		_, err = testCtx.S3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).To(HaveOccurred(), "DELETE non-empty bucket should fail")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", "block-delete.txt", 200),
			testCtx.BucketOp("REST.DELETE.BUCKET", 409).WithErrorCode("BucketNotEmpty"),
		)
	})

	It("logs NoSuchUpload error for invalid multipart upload ID", func(ctx context.Context) {
		testKey := "no-such-upload.txt"
		fakeUploadID := "00000000-0000-0000-0000-000000000000"

		_, err := testCtx.S3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(testCtx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: aws.String(fakeUploadID),
		})
		Expect(err).To(HaveOccurred(), "Abort with invalid upload ID should fail")

		_, err = testCtx.S3Client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:   aws.String(testCtx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: aws.String(fakeUploadID),
		})
		Expect(err).To(HaveOccurred(), "ListParts with invalid upload ID should fail")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.DELETE.UPLOAD", testKey, 404).WithErrorCode("NoSuchUpload"),
			testCtx.ObjectOp("REST.GET.UPLOAD", testKey, 404).WithErrorCode("NoSuchUpload"),
		)
	})
})
