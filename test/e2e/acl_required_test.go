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

		_, err = testCtx.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET should succeed")

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

		userName := fmt.Sprintf("e2e-acl-test-%d", time.Now().UnixNano())
		_, err = iamClient.CreateUser(ctx, &iam.CreateUserInput{
			UserName: aws.String(userName),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateUser should succeed")

		createKeyResp, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
			UserName: aws.String(userName),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateAccessKey should succeed")

		// Attach inline policy allowing s3:GetObject on the source bucket
		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::%s/*"
			}]
		}`, testCtx.SourceBucket)
		_, err = iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
			UserName:       aws.String(userName),
			PolicyName:     aws.String("allow-get-object"),
			PolicyDocument: aws.String(policy),
		})
		Expect(err).NotTo(HaveOccurred(), "PutUserPolicy should succeed")

		defer func() {
			_, _ = iamClient.DeleteUserPolicy(ctx, &iam.DeleteUserPolicyInput{
				UserName:   aws.String(userName),
				PolicyName: aws.String("allow-get-object"),
			})
			_, _ = iamClient.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
				UserName:    aws.String(userName),
				AccessKeyId: createKeyResp.AccessKey.AccessKeyId,
			})
			_, _ = iamClient.DeleteUser(ctx, &iam.DeleteUserInput{
				UserName: aws.String(userName),
			})
		}()

		// GET object as the IAM user — authorized via ACL (same account)
		iamS3Client := newS3ClientWithCredentials(
			*createKeyResp.AccessKey.AccessKeyId,
			*createKeyResp.AccessKey.SecretAccessKey,
		)

		_, err = iamS3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET as IAM user should succeed")

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

		userName := fmt.Sprintf("e2e-acl-copy-%d", time.Now().UnixNano())
		_, err = iamClient.CreateUser(ctx, &iam.CreateUserInput{
			UserName: aws.String(userName),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateUser should succeed")

		createKeyResp, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
			UserName: aws.String(userName),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateAccessKey should succeed")

		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": ["s3:GetObject", "s3:PutObject"],
				"Resource": "arn:aws:s3:::%s/*"
			}]
		}`, testCtx.SourceBucket)
		_, err = iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
			UserName:       aws.String(userName),
			PolicyName:     aws.String("allow-copy"),
			PolicyDocument: aws.String(policy),
		})
		Expect(err).NotTo(HaveOccurred(), "PutUserPolicy should succeed")

		defer func() {
			_, _ = iamClient.DeleteUserPolicy(ctx, &iam.DeleteUserPolicyInput{
				UserName:   aws.String(userName),
				PolicyName: aws.String("allow-copy"),
			})
			_, _ = iamClient.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
				UserName:    aws.String(userName),
				AccessKeyId: createKeyResp.AccessKey.AccessKeyId,
			})
			_, _ = iamClient.DeleteUser(ctx, &iam.DeleteUserInput{
				UserName: aws.String(userName),
			})
		}()

		// Copy as IAM user — both source GET and destination PUT go through ACL path
		iamS3Client := newS3ClientWithCredentials(
			*createKeyResp.AccessKey.AccessKeyId,
			*createKeyResp.AccessKey.SecretAccessKey,
		)

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
