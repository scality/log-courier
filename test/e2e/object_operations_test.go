package e2e_test

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object Operations", func() {
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	Describe("Basic Object operations", func() {
		It("should log put, get, head, and delete operations", func() {
			testKey := "test-object.txt"
			testContent := []byte("test data for basic CRUD operations")

			// Perform operations
			By("performing put operation")
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
				Body:   bytes.NewReader(testContent),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

			By("performing get operation")
			_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
			})
			Expect(err).NotTo(HaveOccurred(), "GET operation should succeed")

			By("performing head operation")
			_, err = ctx.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
			})
			Expect(err).NotTo(HaveOccurred(), "HEAD operation should succeed")

			By("performing delete operation")
			_, err = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(testKey),
			})
			Expect(err).NotTo(HaveOccurred(), "DELETE operation should succeed")

			// Wait for logs to appear (4 operations)
			By("waiting for logs to appear in destination bucket")
			logs := waitForLogCount(ctx, 4, 45*time.Second)

			// Verify each operation in order
			By("verifying put operation log")
			verifyLogRecord(logs[0], ExpectedLog{
				Operation:  "REST.PUT.OBJECT",
				Bucket:     ctx.SourceBucket,
				Key:        testKey,
				HTTPStatus: 200,
			})

			By("verifying get operation log")
			verifyLogRecord(logs[1], ExpectedLog{
				Operation:  "REST.GET.OBJECT",
				Bucket:     ctx.SourceBucket,
				Key:        testKey,
				HTTPStatus: 200,
			})

			By("verifying head operation log")
			verifyLogRecord(logs[2], ExpectedLog{
				Operation:  "REST.HEAD.OBJECT",
				Bucket:     ctx.SourceBucket,
				Key:        testKey,
				HTTPStatus: 200,
			})

			By("verifying delete operation log")
			verifyLogRecord(logs[3], ExpectedLog{
				Operation:  "REST.DELETE.OBJECT",
				Bucket:     ctx.SourceBucket,
				Key:        testKey,
				HTTPStatus: 204,
			})

			// Verify chronological order
			By("verifying logs are in chronological order")
			verifyChronologicalOrder(logs)
		})
	})
})
