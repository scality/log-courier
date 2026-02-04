package e2e_test

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Multipart Operations", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs complete multipart upload flow", func(ctx context.Context) {
		testKey := "multipart-test-object.txt"
		part1Data := bytes.Repeat([]byte("a"), 5*1024*1024) // 5MB
		part2Data := bytes.Repeat([]byte("b"), 5*1024*1024) // 5MB

		createResp, err := testCtx.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "Create multipart upload should succeed")
		uploadID := createResp.UploadId

		part1Resp, err := testCtx.S3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(testCtx.SourceBucket),
			Key:        aws.String(testKey),
			PartNumber: aws.Int32(1),
			UploadId:   uploadID,
			Body:       bytes.NewReader(part1Data),
		})
		Expect(err).NotTo(HaveOccurred(), "Upload part 1 should succeed")

		part2Resp, err := testCtx.S3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(testCtx.SourceBucket),
			Key:        aws.String(testKey),
			PartNumber: aws.Int32(2),
			UploadId:   uploadID,
			Body:       bytes.NewReader(part2Data),
		})
		Expect(err).NotTo(HaveOccurred(), "Upload part 2 should succeed")

		_, err = testCtx.S3Client.ListParts(ctx, &s3.ListPartsInput{
			Bucket:   aws.String(testCtx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: uploadID,
		})
		Expect(err).NotTo(HaveOccurred(), "List parts should succeed")

		_, err = testCtx.S3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(testCtx.SourceBucket),
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

		expectedSize := int64(len(part1Data) + len(part2Data))
		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.POST.UPLOADS", testKey, 200),
			testCtx.ObjectOp("REST.PUT.PART", testKey, 200),
			testCtx.ObjectOp("REST.PUT.PART", testKey, 200),
			testCtx.ObjectOp("REST.GET.UPLOAD", testKey, 200),
			testCtx.ObjectOp("REST.POST.UPLOAD", testKey, 200).WithObjectSize(expectedSize),
		)
	})

	It("logs multipart abort and list uploads", func(ctx context.Context) {
		testKey := "multipart-abort-test.txt"
		partData := bytes.Repeat([]byte("x"), 5*1024*1024) // 5MB

		createResp, err := testCtx.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "Create multipart upload should succeed")
		uploadID := createResp.UploadId

		_, err = testCtx.S3Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(testCtx.SourceBucket),
			Key:        aws.String(testKey),
			PartNumber: aws.Int32(1),
			UploadId:   uploadID,
			Body:       bytes.NewReader(partData),
		})
		Expect(err).NotTo(HaveOccurred(), "Upload part should succeed")

		_, err = testCtx.S3Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "List multipart uploads should succeed")

		_, err = testCtx.S3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(testCtx.SourceBucket),
			Key:      aws.String(testKey),
			UploadId: uploadID,
		})
		Expect(err).NotTo(HaveOccurred(), "Abort multipart upload should succeed")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.POST.UPLOADS", testKey, 200),
			testCtx.ObjectOp("REST.PUT.PART", testKey, 200),
			testCtx.BucketOp("REST.GET.UPLOADS", 200),
			testCtx.ObjectOp("REST.DELETE.UPLOAD", testKey, 204),
		)
	})

	It("logs multipart upload with copy part operations", func(ctx context.Context) {
		sourceKey := "multipart-source.txt"
		destKey := "multipart-dest.txt"
		sourceContent := bytes.Repeat([]byte("a"), 5*1024*1024)

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(sourceKey),
			Body:   bytes.NewReader(sourceContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT source object should succeed")

		initResp, err := testCtx.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(destKey),
		})
		Expect(err).NotTo(HaveOccurred(), "Initiate multipart upload should succeed")
		uploadID := initResp.UploadId

		copyPartResp, err := testCtx.S3Client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:     aws.String(testCtx.SourceBucket),
			Key:        aws.String(destKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", testCtx.SourceBucket, sourceKey)),
			PartNumber: aws.Int32(1),
			UploadId:   uploadID,
		})
		Expect(err).NotTo(HaveOccurred(), "UploadPartCopy should succeed")

		_, err = testCtx.S3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(testCtx.SourceBucket),
			Key:      aws.String(destKey),
			UploadId: uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: []types.CompletedPart{
					{
						ETag:       copyPartResp.CopyPartResult.ETag,
						PartNumber: aws.Int32(1),
					},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "Complete multipart upload should succeed")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", sourceKey, 200).WithObjectSize(int64(len(sourceContent))),
			testCtx.ObjectOp("REST.POST.UPLOADS", destKey, 200),
			testCtx.ObjectOp("REST.COPY.PART_GET", sourceKey, 200),
			testCtx.ObjectOp("REST.COPY.PART", destKey, 200),
			testCtx.ObjectOp("REST.POST.UPLOAD", destKey, 200).WithObjectSize(int64(len(sourceContent))),
		)
	})
})
