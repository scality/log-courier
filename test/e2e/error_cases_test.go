package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
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

	// Authentication failures (SignatureDoesNotMatch, InvalidAccessKeyId) are not
	// written to bucket access logs.
	It("does not log SignatureDoesNotMatch errors", func(ctx context.Context) {
		testKey := "auth-test-object.txt"
		testContent := []byte("auth test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT object should succeed")

		accessKey := os.Getenv("E2E_S3_ACCESS_KEY_ID")
		if accessKey == "" {
			accessKey = testAccessKeyID
		}
		wrongSecretClient := newS3ClientWithCredentials(accessKey, "wrong-secret-key-for-testing")

		_, err = wrongSecretClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).To(HaveOccurred(), "GET with wrong secret key should fail")

		// Only the PUT should be logged; the auth failure is not logged
		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
		)
	})

	It("does not log InvalidAccessKeyId errors", func(ctx context.Context) {
		testKey := "invalid-key-test.txt"
		testContent := []byte("invalid key test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT object should succeed")

		invalidKeyClient := newS3ClientWithCredentials("INVALIDACCESSKEY12345", "invalid-secret-key")

		_, err = invalidKeyClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).To(HaveOccurred(), "GET with invalid access key should fail")

		// Only the PUT should be logged; the auth failure is not logged
		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
		)
	})

	It("logs AccessDenied error for IAM user without permissions", func(ctx context.Context) {
		testKey := "access-denied-test.txt"
		testContent := []byte("access denied test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT object should succeed")

		// Sleep to ensure the PUT gets a strictly earlier timestamp than the GET
		// (log timestamps have second granularity)
		// This prevents flakiness.
		time.Sleep(1 * time.Second)

		// Create IAM user with no permissions
		iamEndpoint := os.Getenv("E2E_IAM_ENDPOINT")
		if iamEndpoint == "" {
			iamEndpoint = testIAMEndpoint
		}
		accessKey := os.Getenv("E2E_S3_ACCESS_KEY_ID")
		if accessKey == "" {
			accessKey = testAccessKeyID
		}
		secretKey := os.Getenv("E2E_S3_SECRET_ACCESS_KEY")
		if secretKey == "" {
			secretKey = testSecretAccessKey
		}

		iamClient := iam.NewFromConfig(aws.Config{
			Region: testRegion,
			Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretKey,
				}, nil
			}),
		}, func(o *iam.Options) {
			o.BaseEndpoint = aws.String(iamEndpoint)
		})

		userName := fmt.Sprintf("e2e-test-user-%d", time.Now().UnixNano())
		_, err = iamClient.CreateUser(ctx, &iam.CreateUserInput{
			UserName: aws.String(userName),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateUser should succeed")

		createKeyResp, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
			UserName: aws.String(userName),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateAccessKey should succeed")

		defer func() {
			_, _ = iamClient.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
				UserName:    aws.String(userName),
				AccessKeyId: createKeyResp.AccessKey.AccessKeyId,
			})
			_, _ = iamClient.DeleteUser(ctx, &iam.DeleteUserInput{
				UserName: aws.String(userName),
			})
		}()

		unprivilegedClient := newS3ClientWithCredentials(
			*createKeyResp.AccessKey.AccessKeyId,
			*createKeyResp.AccessKey.SecretAccessKey,
		)

		_, err = unprivilegedClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).To(HaveOccurred(), "GET with unprivileged user should fail")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			testCtx.ObjectOp("REST.GET.OBJECT", testKey, 403).WithErrorCode("AccessDenied"),
		)
	})
})
