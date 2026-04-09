package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("aclRequired field in access logs", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs aclRequired as dash for bucket owner requests", func(ctx context.Context) {
		testKey := "acl-owner-test.txt"
		testContent := []byte("owner request test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		resp, err := testCtx.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET should succeed")
		_ = resp.Body.Close()

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("-"),
			testCtx.ObjectOp("REST.GET.OBJECT", testKey, 200).
				WithBytesSent(int64(len(testContent))).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("-"),
		)
	})

	It("logs aclRequired as Yes for IAM user authorized by ACL", func(ctx context.Context) {
		testKey := "acl-iam-test.txt"
		testContent := []byte("IAM user ACL test data")

		// PUT object as bucket owner
		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		// Create IAM user with s3:GetObject permission
		userName := fmt.Sprintf("e2e-acl-test-%d", time.Now().UnixNano())
		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::%s/*"
			}]
		}`, testCtx.SourceBucket)
		iamUser := createIAMUser(ctx, userName, "allow-get-object", policy)
		defer iamUser.Cleanup()

		// GET object as the IAM user — authorized via ACL (same account)
		iamS3Client := iamUser.S3Client

		resp, err := iamS3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET as IAM user should succeed")
		_ = resp.Body.Close()

		logs := testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("-"),
			testCtx.ObjectOp("REST.GET.OBJECT", testKey, 200).
				WithBytesSent(int64(len(testContent))).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("Yes"),
		)

		// Additional verification: the two requests have different requesters
		Expect(logs[0].Requester).NotTo(Equal(logs[1].Requester),
			"PUT (owner) and GET (IAM user) should have different requesters")
	})

	It("logs aclRequired as Yes for both sides of a copy by IAM user", func(ctx context.Context) {
		sourceKey := "acl-copy-source.txt"
		destKey := "acl-copy-dest.txt"
		testContent := []byte("copy test data")

		// PUT object as bucket owner
		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(sourceKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		// Create IAM user with GetObject + PutObject permissions for the copy
		userName := fmt.Sprintf("e2e-acl-copy-%d", time.Now().UnixNano())
		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": ["s3:GetObject", "s3:PutObject"],
				"Resource": "arn:aws:s3:::%s/*"
			}]
		}`, testCtx.SourceBucket)
		iamUser := createIAMUser(ctx, userName, "allow-copy", policy)
		defer iamUser.Cleanup()

		// Copy as IAM user — both source GET and destination PUT go through ACL path
		iamS3Client := iamUser.S3Client

		_, err = iamS3Client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(testCtx.SourceBucket),
			Key:        aws.String(destKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", testCtx.SourceBucket, sourceKey)),
		})
		Expect(err).NotTo(HaveOccurred(), "COPY as IAM user should succeed")

		testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", sourceKey, 200).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("-"),
			testCtx.ObjectOp("REST.COPY.OBJECT_GET", sourceKey, 200).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("Yes"),
			testCtx.ObjectOp("REST.COPY.OBJECT", destKey, 200).
				WithObjectSize(int64(len(testContent))).
				WithACLRequired("Yes"),
		)
	})
})
