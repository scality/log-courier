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
	"github.com/aws/aws-sdk-go-v2/service/sts"
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

	It("logs the assumed-role ARN for STS AssumeRole sessions", func(ctx context.Context) {
		testKey := "requester-sts-test.txt"
		testContent := []byte("assumed-role requester test data")

		_, err := testCtx.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT should succeed")

		timestamp := time.Now().UnixNano()
		userName := fmt.Sprintf("e2e-sts-user-%d", timestamp)
		roleName := fmt.Sprintf("e2e-sts-role-%d", timestamp)
		sessionName := fmt.Sprintf("e2e-sts-session-%d", timestamp)

		assumeRolePolicy := `{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": "sts:AssumeRole",
				"Resource": "*"
			}]
		}`
		iamUser := createIAMUser(ctx, userName, "allow-assume-role", assumeRolePolicy)
		defer iamUser.Cleanup()

		iamEndpoint := os.Getenv("E2E_IAM_ENDPOINT")
		if iamEndpoint == "" {
			iamEndpoint = testIAMEndpoint
		}
		adminIAMClient := iam.NewFromConfig(aws.Config{
			Region: testRegion,
			Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     testAccessKeyID,
					SecretAccessKey: testSecretAccessKey,
				}, nil
			}),
		}, func(o *iam.Options) {
			o.BaseEndpoint = aws.String(iamEndpoint)
		})

		trustPolicy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Principal": {"AWS": "arn:aws:iam::000000000000:user/%s"},
				"Action": "sts:AssumeRole"
			}]
		}`, userName)
		_, err = adminIAMClient.CreateRole(ctx, &iam.CreateRoleInput{
			RoleName:                 aws.String(roleName),
			AssumeRolePolicyDocument: aws.String(trustPolicy),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateRole should succeed")

		policyName := fmt.Sprintf("e2e-sts-policy-%d", timestamp)
		rolePermissionPolicy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [{
				"Effect": "Allow",
				"Action": "s3:GetObject",
				"Resource": "arn:aws:s3:::%s/*"
			}]
		}`, testCtx.SourceBucket)
		createPolicyResp, err := adminIAMClient.CreatePolicy(ctx, &iam.CreatePolicyInput{
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(rolePermissionPolicy),
		})
		Expect(err).NotTo(HaveOccurred(), "CreatePolicy should succeed")
		policyArn := *createPolicyResp.Policy.Arn
		defer func() {
			_, _ = adminIAMClient.DetachRolePolicy(context.Background(), &iam.DetachRolePolicyInput{
				RoleName:  aws.String(roleName),
				PolicyArn: aws.String(policyArn),
			})
			_, _ = adminIAMClient.DeleteRole(context.Background(), &iam.DeleteRoleInput{
				RoleName: aws.String(roleName),
			})
			_, _ = adminIAMClient.DeletePolicy(context.Background(), &iam.DeletePolicyInput{
				PolicyArn: aws.String(policyArn),
			})
		}()

		_, err = adminIAMClient.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
			RoleName:  aws.String(roleName),
			PolicyArn: aws.String(policyArn),
		})
		Expect(err).NotTo(HaveOccurred(), "AttachRolePolicy should succeed")

		stsEndpoint := os.Getenv("E2E_STS_ENDPOINT")
		if stsEndpoint == "" {
			stsEndpoint = testSTSEndpoint
		}
		stsClient := sts.NewFromConfig(aws.Config{
			Region: testRegion,
			Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
				return aws.Credentials{
					AccessKeyID:     iamUser.AccessKeyID,
					SecretAccessKey: iamUser.SecretAccessKey,
				}, nil
			}),
		}, func(o *sts.Options) {
			o.BaseEndpoint = aws.String(stsEndpoint)
		})

		roleArn := fmt.Sprintf("arn:aws:iam::000000000000:role/%s", roleName)
		var assumeResp *sts.AssumeRoleOutput
		Eventually(func() error {
			var assumeErr error
			assumeResp, assumeErr = stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
				RoleArn:         aws.String(roleArn),
				RoleSessionName: aws.String(sessionName),
			})
			return assumeErr
		}, 10*time.Second, 500*time.Millisecond).Should(Succeed(),
			"AssumeRole should succeed once role propagation completes")

		roleS3Client := newS3ClientWithCredentials(
			*assumeResp.Credentials.AccessKeyId,
			*assumeResp.Credentials.SecretAccessKey,
			*assumeResp.Credentials.SessionToken,
		)

		resp, err := roleS3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET as assumed role should succeed")
		_ = resp.Body.Close()

		logs := testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).
				WithObjectSize(int64(len(testContent))),
			testCtx.ObjectOp("REST.GET.OBJECT", testKey, 200).
				WithBytesSent(int64(len(testContent))).
				WithObjectSize(int64(len(testContent))),
		)

		expectedARN := fmt.Sprintf("arn:aws:sts::000000000000:assumed-role/%s/%s", roleName, sessionName)
		Expect(logs[1].Requester).To(Equal(expectedARN),
			"GET as assumed role should log the STS assumed-role ARN as the Requester")
	})
})
