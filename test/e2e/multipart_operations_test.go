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

var _ = Describe("Multipart Operations", func() {
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("logs complete multipart upload flow", func() {
		testKey := "multipart-test-object.txt"
		part1Data := bytes.Repeat([]byte("a"), 5*1024*1024) // 5MB
		part2Data := bytes.Repeat([]byte("b"), 5*1024*1024) // 5MB

		// Create Multipart Upload
		By("creating multipart upload")
		createResp, err := ctx.S3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "Create multipart upload should succeed")
		uploadID := createResp.UploadId

		// Upload Part 1
		By("uploading part 1")
		part1Resp, err := ctx.S3Client.UploadPart(context.Background(), &s3.UploadPartInput{
			Bucket:     aws.String(ctx.SourceBucket),
			Key:        aws.String(testKey),
			PartNumber: aws.Int32(1),
			UploadId:   uploadID,
			Body:       bytes.NewReader(part1Data),
		})
		Expect(err).NotTo(HaveOccurred(), "Upload part 1 should succeed")

		// Upload Part 2
		By("uploading part 2")
		part2Resp, err := ctx.S3Client.UploadPart(context.Background(), &s3.UploadPartInput{
			Bucket:     aws.String(ctx.SourceBucket),
			Key:        aws.String(testKey),
			PartNumber: aws.Int32(2),
			UploadId:   uploadID,
			Body:       bytes.NewReader(part2Data),
		})
		Expect(err).NotTo(HaveOccurred(), "Upload part 2 should succeed")

		// List Parts
		By("listing parts")
		_, err = ctx.S3Client.ListParts(context.Background(), &s3.ListPartsInput{
			Bucket:   aws.String(ctx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: uploadID,
		})
		Expect(err).NotTo(HaveOccurred(), "List parts should succeed")

		// Complete Multipart Upload
		By("completing multipart upload")
		_, err = ctx.S3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(ctx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       part1Resp.ETag,
						PartNumber: aws.Int32(1),
					},
					{
						ETag:       part2Resp.ETag,
						PartNumber: aws.Int32(2),
					},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "Complete multipart upload should succeed")

		// Wait for logs (3 operations: create, list parts, complete)
		// TODO: S3C-10730. Put part operations are not logged.
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 3)

		// Verify operations
		By("verifying create multipart upload log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.POST.UPLOADS",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying list parts log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.UPLOAD",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying complete multipart upload log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.POST.UPLOAD",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("logs multipart abort and list uploads", func() {
		testKey := "multipart-abort-test.txt"
		partData := bytes.Repeat([]byte("x"), 5*1024*1024) // 5MB

		// Create Multipart Upload
		By("creating multipart upload")
		createResp, err := ctx.S3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "Create multipart upload should succeed")
		uploadID := createResp.UploadId

		// Upload Part
		By("uploading part")
		_, err = ctx.S3Client.UploadPart(context.Background(), &s3.UploadPartInput{
			Bucket:     aws.String(ctx.SourceBucket),
			Key:        aws.String(testKey),
			PartNumber: aws.Int32(1),
			UploadId:   uploadID,
			Body:       bytes.NewReader(partData),
		})
		Expect(err).NotTo(HaveOccurred(), "Upload part should succeed")

		// List Multipart Uploads
		By("listing multipart uploads")
		_, err = ctx.S3Client.ListMultipartUploads(context.Background(), &s3.ListMultipartUploadsInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "List multipart uploads should succeed")

		// Abort Multipart Upload
		By("aborting multipart upload")
		_, err = ctx.S3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(ctx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: uploadID,
		})
		Expect(err).NotTo(HaveOccurred(), "Abort multipart upload should succeed")

		// Wait for logs (3 operations: create, list uploads, abort)
		// TODO: S3C-10730. Put part operations are not logged.
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 3)

		// Verify operations
		By("verifying create multipart upload log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.POST.UPLOADS",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying list multipart uploads log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.UPLOADS",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying abort multipart upload log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.DELETE.UPLOAD",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})
})
