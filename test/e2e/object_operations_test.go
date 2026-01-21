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

var _ = Describe("Object Operations", func() {
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("logs basic CRUD operations (PUT/GET/HEAD/DELETE)", func() {
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
		logs := waitForLogCount(ctx, 4)

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

	It("logs PUT operations with ACL and metadata", func() {
		keyWithACL := "object-with-acl.txt"
		keyWithMeta := "object-with-metadata.txt"
		testContent := []byte("test data")

		// PUT Object with ACL
		By("performing put with ACL")
		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(keyWithACL),
			Body:   bytes.NewReader(testContent),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT with ACL should succeed")

		// PUT Object with Metadata
		By("performing put with metadata")
		_, err = ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(keyWithMeta),
			Body:   bytes.NewReader(testContent),
			Metadata: map[string]string{
				"custom-key": "custom-value",
				"author":     "test-suite",
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT with metadata should succeed")

		// Wait for logs (2 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 2)

		// Verify operations
		By("verifying put with ACL log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        keyWithACL,
			HTTPStatus: 200,
		})

		By("verifying put with metadata log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.PUT.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        keyWithMeta,
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("logs list and copy operations", func() {
		// Create multiple objects
		By("creating multiple objects")
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("list-test/object-%d.txt", i)
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("content-%d", i))),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT object %d should succeed", i)
		}

		// List all objects
		By("listing all objects")
		_, err := ctx.S3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "LIST objects should succeed")

		// List objects with prefix
		By("listing objects with prefix")
		_, err = ctx.S3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(ctx.SourceBucket),
			Prefix: aws.String("list-test/"),
		})
		Expect(err).NotTo(HaveOccurred(), "LIST with prefix should succeed")

		// Copy object
		By("copying an object")
		sourceKey := "list-test/object-1.txt"
		destKey := "list-test/object-1-copy.txt"
		_, err = ctx.S3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
			Bucket:     aws.String(ctx.SourceBucket),
			Key:        aws.String(destKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", ctx.SourceBucket, sourceKey)),
		})
		Expect(err).NotTo(HaveOccurred(), "COPY object should succeed")

		// Wait for logs (5 PUTs + 2 LISTs + 1 COPY = 8 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 8)

		// Verify PUT operations (first 5 logs)
		By("verifying PUT operations")
		for i := 0; i < 5; i++ {
			expectedKey := fmt.Sprintf("list-test/object-%d.txt", i+1)
			verifyLogRecord(logs[i], ExpectedLog{
				Operation:  "REST.PUT.OBJECT",
				Bucket:     ctx.SourceBucket,
				Key:        expectedKey,
				HTTPStatus: 200,
			})
		}

		// Verify LIST operations (next 2 logs)
		By("verifying first LIST operation")
		verifyLogRecord(logs[5], ExpectedLog{
			Operation:  "REST.GET.BUCKET",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying second LIST operation")
		verifyLogRecord(logs[6], ExpectedLog{
			Operation:  "REST.GET.BUCKET",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		// Verify COPY operation (last log)
		By("verifying COPY operation")
		verifyLogRecord(logs[7], ExpectedLog{
			Operation:  "REST.PUT.COPY",
			Bucket:     ctx.SourceBucket,
			Key:        destKey,
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("logs object ACL operations", func() {
		testKey := "acl-test-object.txt"
		testContent := []byte("test data for ACL operations")

		// PUT Object
		By("performing put operation")
		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		// GET Object ACL
		By("getting object ACL")
		_, err = ctx.S3Client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET ACL should succeed")

		// PUT Object ACL
		By("putting object ACL")
		_, err = ctx.S3Client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT ACL should succeed")

		// GET Object ACL again to verify
		By("getting object ACL again to verify")
		_, err = ctx.S3Client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET ACL verification should succeed")

		// Wait for logs (4 operations: PUT + GET ACL + PUT ACL + GET ACL)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 4)

		// Verify operations
		By("verifying PUT operation log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying GET ACL operation log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.ACL",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying PUT ACL operation log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.PUT.ACL",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying second GET ACL operation log")
		verifyLogRecord(logs[3], ExpectedLog{
			Operation:  "REST.GET.ACL",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("logs object tagging operations", func() {
		testKey := "tagging-test-object.txt"
		testContent := []byte("test data for tagging operations")

		// PUT Object
		By("performing put operation")
		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		// PUT Object Tagging
		By("putting object tagging")
		_, err = ctx.S3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{
					{Key: aws.String("Environment"), Value: aws.String("test")},
					{Key: aws.String("Owner"), Value: aws.String("e2e-suite")},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT tagging should succeed")

		// GET Object Tagging
		By("getting object tagging")
		_, err = ctx.S3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET tagging should succeed")

		// DELETE Object Tagging
		By("deleting object tagging")
		_, err = ctx.S3Client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE tagging should succeed")

		// Wait for logs (4 operations: PUT + PUT tagging + GET tagging + DELETE tagging)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 4)

		// Verify operations
		By("verifying PUT operation log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.OBJECT",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying PUT tagging operation log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.PUT.TAGGING",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying GET tagging operation log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.GET.TAGGING",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 200,
		})

		By("verifying DELETE tagging operation log")
		verifyLogRecord(logs[3], ExpectedLog{
			Operation:  "REST.DELETE.TAGGING",
			Bucket:     ctx.SourceBucket,
			Key:        testKey,
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})
})
