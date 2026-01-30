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

		By("creating test object")
		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT test object should succeed")

		// GET non-existent object (404)
		By("getting non-existent object (expecting 404)")
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		Expect(err).To(HaveOccurred(), "GET non-existent object should fail")

		// DELETE non-existent object (404)
		By("deleting non-existent object (expecting 404)")
		_, _ = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		// Note: DELETE on non-existent objects typically returns 204, not 404
		// So we don't check for error here

		// HEAD non-existent object (404)
		By("heading non-existent object (expecting 404)")
		_, err = ctx.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(nonExistentKey),
		})
		Expect(err).To(HaveOccurred(), "HEAD non-existent object should fail")

		// Wait for logs (4 operations: 1 PUT + 1 GET 404 + 1 DELETE + 1 HEAD 404)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 4)

		// Verify operations
		By("verifying PUT test object log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying GET non-existent object log (404)")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        nonExistentKey,
			HTTPStatus: 404,
			ErrorCode:  "NoSuchKey",
		})

		By("verifying timing fields are logged even for error responses")
		Expect(logs[1].TotalTime).To(BeNumerically(">", 0),
			"TotalTime should be positive even for 404")
		Expect(logs[1].TurnAroundTime).To(BeNumerically(">=", 0),
			"TurnAroundTime should be non-negative for 404")

		By("verifying DELETE non-existent object log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.DELETE.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        nonExistentKey,
			HTTPStatus: 204,
		})

		By("verifying HEAD non-existent object log (404)")
		verifyLogRecord(logs[3], ExpectedLog{
			Operation:  "REST.HEAD.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        nonExistentKey,
			HTTPStatus: 404,
			ErrorCode:  "NoSuchKey",
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})
})
