package e2e_test

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket Configuration", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs bucket logging configuration operations", func() {
		_, err := testCtx.S3Client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket logging should succeed")

		_, err = testCtx.S3Client.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{
			Bucket: aws.String(testCtx.SourceBucket),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(testCtx.DestinationBucket),
					TargetPrefix: aws.String("updated-logs/"),
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket logging should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.GET.LOGGING_STATUS", 200),
			testCtx.BucketOp("REST.PUT.LOGGING_STATUS", 200),
		)
	})

	It("logs bucket policy operations", func() {
		bucketPolicy := `{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Principal": "*",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::` + testCtx.SourceBucket + `/*"
				}]
			}`

		_, err := testCtx.S3Client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Policy: aws.String(bucketPolicy),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket policy should succeed")

		_, err = testCtx.S3Client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket policy should succeed")

		_, err = testCtx.S3Client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket policy should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.PUT.BUCKETPOLICY", 200),
			testCtx.BucketOp("REST.GET.BUCKETPOLICY", 200),
			testCtx.BucketOp("REST.DELETE.BUCKETPOLICY", 204),
		)
	})

	It("logs bucket CORS configuration operations", func() {
		corsConfig := &types.CORSConfiguration{
			CORSRules: []types.CORSRule{
				{
					AllowedMethods: []string{"GET", "PUT"},
					AllowedOrigins: []string{"*"},
					AllowedHeaders: []string{"*"},
					MaxAgeSeconds:  aws.Int32(3000),
				},
			},
		}

		_, err := testCtx.S3Client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
			Bucket:            aws.String(testCtx.SourceBucket),
			CORSConfiguration: corsConfig,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket CORS should succeed")

		_, err = testCtx.S3Client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket CORS should succeed")

		_, err = testCtx.S3Client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket CORS should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.PUT.CORS", 200),
			testCtx.BucketOp("REST.GET.CORS", 200),
			testCtx.BucketOp("REST.DELETE.CORS", 204),
		)
	})

	It("logs bucket website configuration operations", func() {
		websiteConfig := &types.WebsiteConfiguration{
			IndexDocument: &types.IndexDocument{
				Suffix: aws.String("index.html"),
			},
			ErrorDocument: &types.ErrorDocument{
				Key: aws.String("error.html"),
			},
		}

		_, err := testCtx.S3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
			Bucket:               aws.String(testCtx.SourceBucket),
			WebsiteConfiguration: websiteConfig,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket website should succeed")

		_, err = testCtx.S3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket website should succeed")

		_, err = testCtx.S3Client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket website should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.PUT.WEBSITE", 200),
			testCtx.BucketOp("REST.GET.WEBSITE", 200),
			testCtx.BucketOp("REST.DELETE.WEBSITE", 204),
		)
	})

	It("logs bucket lifecycle configuration operations", func() {
		lifecycleConfig := &types.BucketLifecycleConfiguration{
			Rules: []types.LifecycleRule{
				{
					ID:     aws.String("test-rule"),
					Status: types.ExpirationStatusEnabled,
					Prefix: aws.String("temp/"),
					Expiration: &types.LifecycleExpiration{
						Days: aws.Int32(30),
					},
				},
			},
		}

		_, err := testCtx.S3Client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
			Bucket:                 aws.String(testCtx.SourceBucket),
			LifecycleConfiguration: lifecycleConfig,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket lifecycle should succeed")

		_, err = testCtx.S3Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket lifecycle should succeed")

		_, err = testCtx.S3Client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(testCtx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket lifecycle should succeed")

		testCtx.VerifyLogs(
			testCtx.BucketOp("REST.PUT.LIFECYCLE", 200),
			testCtx.BucketOp("REST.GET.LIFECYCLE", 200),
			testCtx.BucketOp("REST.DELETE.LIFECYCLE", 204),
		)
	})
})
