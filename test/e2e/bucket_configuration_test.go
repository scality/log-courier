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
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("logs bucket logging configuration operations", func() {
		// GET Bucket Logging
		By("getting bucket logging configuration")
		_, err := ctx.S3Client.GetBucketLogging(context.Background(), &s3.GetBucketLoggingInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket logging should succeed")

		// PUT Bucket Logging (update configuration)
		By("putting bucket logging configuration")
		_, err = ctx.S3Client.PutBucketLogging(context.Background(), &s3.PutBucketLoggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			BucketLoggingStatus: &types.BucketLoggingStatus{
				LoggingEnabled: &types.LoggingEnabled{
					TargetBucket: aws.String(ctx.DestinationBucket),
					TargetPrefix: aws.String("updated-logs/"),
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket logging should succeed")

		// Wait for logs (2 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 2)

		// Verify operations
		By("verifying get bucket logging log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.GET.LOGGING_STATUS",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying put bucket logging log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.PUT.LOGGING_STATUS",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})

	It("logs bucket policy operations", func() {
		bucketPolicy := `{
				"Version": "2012-10-17",
				"Statement": [{
					"Effect": "Allow",
					"Principal": "*",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::` + ctx.SourceBucket + `/*"
				}]
			}`

		// PUT Bucket Policy
		By("putting bucket policy")
		_, err := ctx.S3Client.PutBucketPolicy(context.Background(), &s3.PutBucketPolicyInput{
			Bucket: aws.String(ctx.SourceBucket),
			Policy: aws.String(bucketPolicy),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket policy should succeed")

		// GET Bucket Policy
		By("getting bucket policy")
		_, err = ctx.S3Client.GetBucketPolicy(context.Background(), &s3.GetBucketPolicyInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket policy should succeed")

		// DELETE Bucket Policy
		By("deleting bucket policy")
		_, err = ctx.S3Client.DeleteBucketPolicy(context.Background(), &s3.DeleteBucketPolicyInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket policy should succeed")

		// Wait for logs (3 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 3)

		// Verify operations
		By("verifying put bucket policy log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.BUCKETPOLICY",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying get bucket policy log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.BUCKETPOLICY",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying delete bucket policy log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.DELETE.BUCKETPOLICY",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
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

		// PUT Bucket CORS
		By("putting bucket CORS configuration")
		_, err := ctx.S3Client.PutBucketCors(context.Background(), &s3.PutBucketCorsInput{
			Bucket:            aws.String(ctx.SourceBucket),
			CORSConfiguration: corsConfig,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket CORS should succeed")

		// GET Bucket CORS
		By("getting bucket CORS configuration")
		_, err = ctx.S3Client.GetBucketCors(context.Background(), &s3.GetBucketCorsInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket CORS should succeed")

		// DELETE Bucket CORS
		By("deleting bucket CORS configuration")
		_, err = ctx.S3Client.DeleteBucketCors(context.Background(), &s3.DeleteBucketCorsInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket CORS should succeed")

		// Wait for logs (3 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 3)

		// Verify operations
		By("verifying put bucket CORS log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.CORS",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying get bucket CORS log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.CORS",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying delete bucket CORS log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.DELETE.CORS",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
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

		// PUT Bucket Website
		By("putting bucket website configuration")
		_, err := ctx.S3Client.PutBucketWebsite(context.Background(), &s3.PutBucketWebsiteInput{
			Bucket:               aws.String(ctx.SourceBucket),
			WebsiteConfiguration: websiteConfig,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket website should succeed")

		// GET Bucket Website
		By("getting bucket website configuration")
		_, err = ctx.S3Client.GetBucketWebsite(context.Background(), &s3.GetBucketWebsiteInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket website should succeed")

		// DELETE Bucket Website
		By("deleting bucket website configuration")
		_, err = ctx.S3Client.DeleteBucketWebsite(context.Background(), &s3.DeleteBucketWebsiteInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket website should succeed")

		// Wait for logs (3 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 3)

		// Verify operations
		By("verifying put bucket website log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.WEBSITE",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying get bucket website log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.WEBSITE",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying delete bucket website log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.DELETE.WEBSITE",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
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

		// PUT Bucket Lifecycle
		By("putting bucket lifecycle configuration")
		_, err := ctx.S3Client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
			Bucket:                 aws.String(ctx.SourceBucket),
			LifecycleConfiguration: lifecycleConfig,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT bucket lifecycle should succeed")

		// GET Bucket Lifecycle
		By("getting bucket lifecycle configuration")
		_, err = ctx.S3Client.GetBucketLifecycleConfiguration(context.Background(), &s3.GetBucketLifecycleConfigurationInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "GET bucket lifecycle should succeed")

		// DELETE Bucket Lifecycle
		By("deleting bucket lifecycle configuration")
		_, err = ctx.S3Client.DeleteBucketLifecycle(context.Background(), &s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE bucket lifecycle should succeed")

		// Wait for logs (3 operations)
		By("waiting for logs to appear in destination bucket")
		logs := waitForLogCount(ctx, 3)

		// Verify operations
		By("verifying put bucket lifecycle log")
		verifyLogRecord(logs[0], ExpectedLog{
			Operation:  "REST.PUT.LIFECYCLE",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying get bucket lifecycle log")
		verifyLogRecord(logs[1], ExpectedLog{
			Operation:  "REST.GET.LIFECYCLE",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 200,
		})

		By("verifying delete bucket lifecycle log")
		verifyLogRecord(logs[2], ExpectedLog{
			Operation:  "REST.DELETE.LIFECYCLE",
			Bucket:     ctx.SourceBucket,
			Key:        "",
			HTTPStatus: 204,
		})

		By("verifying logs are in chronological order")
		verifyChronologicalOrder(logs)
	})
})
