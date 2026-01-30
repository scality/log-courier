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
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("logs error responses (404s)", func() {
		nonExistentKey := "does-not-exist.txt"
		testKey := "error-test-object.txt"
		testContent := []byte("test data")

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT test object should succeed")

		// GET non-existent object (404)
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		Expect(err).To(HaveOccurred(), "GET non-existent object should fail")

		// DELETE non-existent object (404)
		_, _ = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		// Note: DELETE on non-existent objects typically returns 204, not 404
		// So we don't check for error here

		// HEAD non-existent object (404)
		_, err = ctx.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		Expect(err).To(HaveOccurred(), "HEAD non-existent object should fail")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.GET.OBJECT", nonExistentKey, 404).WithErrorCode("NoSuchKey"),
			ctx.ObjectOp("REST.DELETE.OBJECT", nonExistentKey, 204),
			ctx.ObjectOp("REST.HEAD.OBJECT", nonExistentKey, 404).WithErrorCode("NoSuchKey"),
		)
	})
})
