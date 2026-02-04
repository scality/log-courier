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
})
