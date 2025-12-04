package s3_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/s3"
)

func TestS3(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3 Suite")
}

const (
	testKey            = "test-key"
	testBucket         = "test-log-courier"
	testEndpoint       = "http://127.0.0.1:8000"
	testRegion         = "us-east-1"
	workbenchAccessKey = "LSOVSCTL01CME9OETI5A"
	//nolint:gosec // Test credentials
	workbenchSecretKey = "6xHQtgUX46WwfsxyhhdatdWqlZj0omlgVSLx4qNV"
)

// createS3ClientAndBucket creates an AWS S3 client and ensures the bucket exists.
// This is used in test setup to create buckets using the AWS SDK directly.
func createS3ClientAndBucket(ctx context.Context, cfg s3.Config, bucketName string) error {
	s3Client := awss3.NewFromConfig(aws.Config{
		Region: testRegion,
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     cfg.AccessKeyID,
				SecretAccessKey: cfg.SecretAccessKey,
			}, nil
		}),
	}, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})

	_, err := s3Client.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		var bucketAlreadyExists *types.BucketAlreadyExists
		var bucketAlreadyOwnedByYou *types.BucketAlreadyOwnedByYou
		if !errors.As(err, &bucketAlreadyExists) && !errors.As(err, &bucketAlreadyOwnedByYou) {
			return fmt.Errorf("failed to create test bucket: %w", err)
		}
	}

	return nil
}

var _ = Describe("S3 Uploader", func() {
	var (
		ctx      context.Context
		client   *s3.Client
		uploader *s3.Uploader
		bucket   string
	)

	BeforeEach(func() {
		ctx = context.Background()

		logcourier.ConfigSpec.Reset()
		logcourier.ConfigSpec.SetDefault("s3.endpoint", testEndpoint)
		logcourier.ConfigSpec.SetDefault("s3.access-key-id", workbenchAccessKey)
		logcourier.ConfigSpec.SetDefault("s3.secret-access-key", workbenchSecretKey)

		_ = viper.BindEnv("s3.endpoint", "S3_ENDPOINT")
		_ = viper.BindEnv("s3.access-key-id", "S3_ACCESS_KEY_ID")
		_ = viper.BindEnv("s3.secret-access-key", "S3_SECRET_ACCESS_KEY")
		_ = viper.BindEnv("s3.max-retry-attempts", "S3_MAX_RETRY_ATTEMPTS")
		_ = viper.BindEnv("s3.max-backoff-delay-seconds", "S3_MAX_BACKOFF_DELAY_SECONDS")

		bucket = testBucket

		cfg := s3.Config{
			Endpoint:         logcourier.ConfigSpec.GetString("s3.endpoint"),
			AccessKeyID:      logcourier.ConfigSpec.GetString("s3.access-key-id"),
			SecretAccessKey:  logcourier.ConfigSpec.GetString("s3.secret-access-key"),
			MaxRetryAttempts: logcourier.ConfigSpec.GetInt("s3.max-retry-attempts"),
			MaxBackoffDelay:  time.Duration(logcourier.ConfigSpec.GetInt("s3.max-backoff-delay-seconds")) * time.Second,
		}

		var err error
		client, err = s3.NewClient(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())

		err = createS3ClientAndBucket(ctx, cfg, bucket)
		Expect(err).NotTo(HaveOccurred())

		uploader = s3.NewUploader(client)
	})

	Describe("NewClient", func() {
		It("should create client with valid config", func() {
			cfg := s3.Config{
				Endpoint:        testEndpoint,
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "test-secret-key",
			}

			client, err := s3.NewClient(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())
			Expect(client).NotTo(BeNil())
		})

		It("should create client with custom retry configuration", func() {
			cfg := s3.Config{
				Endpoint:         testEndpoint,
				AccessKeyID:      "test-access-key",
				SecretAccessKey:  "test-secret-key",
				MaxRetryAttempts: 5,
				MaxBackoffDelay:  10 * time.Second,
			}

			client, err := s3.NewClient(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())
			Expect(client).NotTo(BeNil())
		})

		It("should fail without access key", func() {
			cfg := s3.Config{
				Endpoint:        testEndpoint,
				AccessKeyID:     "",
				SecretAccessKey: "test-secret-key",
			}

			_, err := s3.NewClient(ctx, cfg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("access key ID and secret access key are required"))
		})

		It("should fail without secret key", func() {
			cfg := s3.Config{
				Endpoint:        testEndpoint,
				AccessKeyID:     "test-access-key",
				SecretAccessKey: "",
			}

			_, err := s3.NewClient(ctx, cfg)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("access key ID and secret access key are required"))
		})
	})

	Describe("Upload", func() {
		It("should upload object successfully", func() {
			key := fmt.Sprintf("test-logs/%d.log", time.Now().UnixNano())
			content := []byte("test log content\n")

			err := uploader.Upload(ctx, bucket, key, content)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should upload empty object", func() {
			key := fmt.Sprintf("test-logs/empty-%d.log", time.Now().UnixNano())
			content := []byte("")

			err := uploader.Upload(ctx, bucket, key, content)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle large objects", func() {
			key := fmt.Sprintf("test-logs/large-%d.log", time.Now().UnixNano())
			// 1MB content
			content := make([]byte, 1024*1024)
			for i := range content {
				content[i] = byte(i % 256)
			}

			err := uploader.Upload(ctx, bucket, key, content)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should fail with invalid bucket", func() {
			content := []byte("test")

			err := uploader.Upload(ctx, "nonexistent-bucket-12345", testKey, content)
			Expect(err).To(HaveOccurred())
		})

		It("should retry on server errors and eventually succeed", func() {
			var requestCount atomic.Int32

			// Mock S3 server that fails first 2 requests, succeeds on 3rd
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := requestCount.Add(1)
				if count < 3 {
					// Return 503 Service Unavailable (retryable error)
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>ServiceUnavailable</Code>
  <Message>Service is temporarily unavailable</Message>
</Error>`))
					return
				}
				// Success on 3rd attempt
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<PutObjectOutput></PutObjectOutput>`))
			}))
			defer mockServer.Close()

			cfg := s3.Config{
				Endpoint:         mockServer.URL,
				AccessKeyID:      workbenchAccessKey,
				SecretAccessKey:  workbenchSecretKey,
				MaxRetryAttempts: 3,
				MaxBackoffDelay:  1 * time.Second, // Short backoff for faster test
			}

			retryClient, err := s3.NewClient(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())

			retryUploader := s3.NewUploader(retryClient)

			content := []byte("test content")

			// Should succeed after retries
			err = retryUploader.Upload(ctx, bucket, testKey, content)
			Expect(err).NotTo(HaveOccurred())

			// Verify exactly 3 requests were made
			Expect(requestCount.Load()).To(Equal(int32(3)))
		})

		It("should exhaust retries and fail when all attempts fail", func() {
			var requestCount atomic.Int32

			// Mock S3 server that always fails
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount.Add(1)
				// Always return 503 Service Unavailable
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>ServiceUnavailable</Code>
  <Message>Service is temporarily unavailable</Message>
</Error>`))
			}))
			defer mockServer.Close()

			cfg := s3.Config{
				Endpoint:         mockServer.URL,
				AccessKeyID:      workbenchAccessKey,
				SecretAccessKey:  workbenchSecretKey,
				MaxRetryAttempts: 3,
				MaxBackoffDelay:  1 * time.Second, // Short backoff for faster test
			}

			retryClient, err := s3.NewClient(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())

			retryUploader := s3.NewUploader(retryClient)

			content := []byte("test content")

			// Should fail after exhausting retries
			err = retryUploader.Upload(ctx, bucket, testKey, content)
			Expect(err).To(HaveOccurred())

			// Verify exactly 3 requests were made (initial + 2 retries)
			Expect(requestCount.Load()).To(Equal(int32(3)))
		})

		It("should respect context timeout for upload operation", func() {
			// Mock S3 server that delays response longer than timeout
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Sleep longer than the test timeout to ensure timeout fires
				time.Sleep(2 * time.Second)
				w.WriteHeader(http.StatusOK)
			}))
			defer mockServer.Close()

			cfg := s3.Config{
				Endpoint:         mockServer.URL,
				AccessKeyID:      workbenchAccessKey,
				SecretAccessKey:  workbenchSecretKey,
				MaxRetryAttempts: 1, // Single attempt
			}

			ctxClient, err := s3.NewClient(ctx, cfg)
			Expect(err).NotTo(HaveOccurred())

			ctxUploader := s3.NewUploader(ctxClient)

			// Create context with short timeout (shorter than server delay)
			uploadCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()

			content := []byte("test content")

			// Should timeout via context before server responds
			err = ctxUploader.Upload(uploadCtx, bucket, testKey, content)
			Expect(err).To(HaveOccurred())
			// Check that error is due to context timeout
			Expect(errors.Is(err, context.DeadlineExceeded) ||
				err.Error() == "context deadline exceeded" ||
				err.Error() == "context canceled").To(BeTrue())
		})
	})
})
