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

var _ = Describe("Requester field in access logs", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs the IAM ARN for IAM user requests", func(ctx context.Context) {
		testKey := "requester-iam-test.txt"
		testContent := []byte("IAM requester test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		userName := fmt.Sprintf("e2e-requester-%d", time.Now().UnixNano())
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

		resp, err := iamUser.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET as IAM user should succeed")
		_ = resp.Body.Close()

		logs := testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).
				WithObjectSize(int64(len(testContent))),
			testCtx.ObjectOp("REST.GET.OBJECT", testKey, 200).
				WithBytesSent(int64(len(testContent))).
				WithObjectSize(int64(len(testContent))),
		)

		expectedARN := fmt.Sprintf("arn:aws:iam::000000000000:user/%s", userName)
		Expect(logs[1].Requester).To(Equal(expectedARN),
			"GET by IAM user should log the IAM ARN as the Requester")
	})

	It("logs the canonical ID for bucket owner requests", func(ctx context.Context) {
		listResp, err := testCtx.S3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
		Expect(err).NotTo(HaveOccurred(), "ListBuckets should succeed")
		Expect(listResp.Owner).NotTo(BeNil(), "ListBuckets should return owner info")
		Expect(listResp.Owner.ID).NotTo(BeNil(), "owner should have a canonical ID")
		expectedCanonicalID := *listResp.Owner.ID

		testKey := "requester-owner-test.txt"
		testContent := []byte("owner requester test data")

		_, err = testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		logs := testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).
				WithObjectSize(int64(len(testContent))),
		)

		Expect(logs[0].Requester).To(Equal(expectedCanonicalID),
			"PUT by bucket owner should log the canonical ID as the Requester")
	})
})
