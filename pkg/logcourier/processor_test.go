package logcourier_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/s3"
	"github.com/scality/log-courier/pkg/testutil"
)

const (
	testS3Endpoint       = "http://127.0.0.1:8000"
	workbenchAccessKey   = "LSOVSCTL01CME9OETI5A"
	workbenchSecretKey   = "6xHQtgUX46WwfsxyhhdatdWqlZj0omlgVSLx4qNV" //nolint:gosec // Test credentials
	testTargetBucket     = "test-log-courier-target"
	testTargetPrefix     = "logs/"
)

var _ = Describe("Processor", func() {
	Describe("Unit Tests", func() {
		Describe("GetMaxInsertedAt", func() {
			It("should return the maximum insertedAt timestamp", func() {
				now := time.Now()
				records := []logcourier.LogRecord{
					{InsertedAt: now.Add(-10 * time.Minute)},
					{InsertedAt: now.Add(-5 * time.Minute)},
					{InsertedAt: now.Add(-15 * time.Minute)},
				}

				result := logcourier.GetMaxInsertedAt(records)
				Expect(result).To(Equal(now.Add(-5 * time.Minute)))
			})

			It("should return zero time for empty records", func() {
				result := logcourier.GetMaxInsertedAt([]logcourier.LogRecord{})
				Expect(result.IsZero()).To(BeTrue())
			})
		})
	})

	Describe("Processor Tests", func() {
		var (
			ctx    context.Context
			helper *testutil.ClickHouseTestHelper
			logger *slog.Logger
		)

		BeforeEach(func() {
			ctx = context.Background()
			logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}))

			var err error
			helper, err = testutil.NewClickHouseTestHelper(ctx)
			Expect(err).NotTo(HaveOccurred())

			err = helper.SetupSchema(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			if helper != nil {
				_ = helper.TeardownSchema(ctx)
				_ = helper.Close()
			}
		})

		Describe("NewProcessor", func() {
			It("should create processor successfully", func() {
				cfg := logcourier.Config{
					Logger:             logger,
					ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
					ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
					ClickHouseDatabase: helper.DatabaseName,
					ClickHouseTimeout:  30 * time.Second,
					CountThreshold:     5,
					TimeThresholdSec:   60,
					DiscoveryInterval:  5 * time.Second,
					NumWorkers:         2,
					MaxRetries:         3,
					InitialBackoff:     1 * time.Second,
					MaxBackoff:         30 * time.Second,
					S3Endpoint:         "http://localhost:8000",
					S3AccessKeyID:      "test-key",
					S3SecretAccessKey:  "test-secret",
				}

				processor, err := logcourier.NewProcessor(ctx, cfg)
				Expect(err).NotTo(HaveOccurred())
				Expect(processor).NotTo(BeNil())

				err = processor.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("should fail with invalid ClickHouse URL", func() {
				cfg := logcourier.Config{
					Logger:             logger,
					ClickHouseHosts:    []string{"invalid://url:9000"},
					ClickHouseUsername: "default",
					ClickHousePassword: "",
					ClickHouseDatabase: helper.DatabaseName,
					ClickHouseTimeout:  30 * time.Second,
					CountThreshold:     5,
					TimeThresholdSec:   60,
					DiscoveryInterval:  5 * time.Second,
					NumWorkers:         2,
					MaxRetries:         3,
					InitialBackoff:     1 * time.Second,
					MaxBackoff:         30 * time.Second,
					S3Endpoint:         "http://localhost:8000",
					S3AccessKeyID:      "test-key",
					S3SecretAccessKey:  "test-secret",
				}

				processor, err := logcourier.NewProcessor(ctx, cfg)
				Expect(err).To(HaveOccurred())
				Expect(processor).To(BeNil())
			})

			It("should fail with empty S3 credentials", func() {
				cfg := logcourier.Config{
					Logger:             logger,
					ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
					ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
					ClickHouseDatabase: helper.DatabaseName,
					ClickHouseTimeout:  30 * time.Second,
					CountThreshold:     5,
					TimeThresholdSec:   60,
					DiscoveryInterval:  5 * time.Second,
					NumWorkers:         2,
					MaxRetries:         3,
					InitialBackoff:     1 * time.Second,
					MaxBackoff:         30 * time.Second,
					S3Endpoint:         "http://localhost:8000",
					S3AccessKeyID:      "",
					S3SecretAccessKey:  "",
				}

				processor, err := logcourier.NewProcessor(ctx, cfg)
				Expect(err).To(HaveOccurred())
				Expect(processor).To(BeNil())
			})
		})

		Describe("End-to-End Batch Processing", func() {
			var (
				s3Helper  *testutil.S3TestHelper
				processor *logcourier.Processor
			)

			BeforeEach(func() {
				// Configure viper for all config keys
				logcourier.ConfigSpec.Reset()
				for key, spec := range logcourier.ConfigSpec {
					viper.SetDefault(key, spec.DefaultValue)
					if spec.EnvVar != "" {
						_ = viper.BindEnv(key, spec.EnvVar)
					}
				}

				// Override S3 config for testing
				viper.Set("s3.endpoint", testS3Endpoint)
				viper.Set("s3.access-key-id", workbenchAccessKey)
				viper.Set("s3.secret-access-key", workbenchSecretKey)

				var err error
				s3Helper, err = testutil.NewS3TestHelper(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Create target bucket
				err = s3Helper.CreateBucket(ctx, testTargetBucket)
				Expect(err).NotTo(HaveOccurred())

				// Create processor
				cfg := logcourier.Config{
					Logger:             logger,
					ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
					ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
					ClickHouseDatabase: helper.DatabaseName,
					ClickHouseTimeout:  30 * time.Second,
					CountThreshold:     5, // Lower threshold for faster tests
					TimeThresholdSec:   60,
					DiscoveryInterval:  5 * time.Second,
					NumWorkers:         2,
					MaxRetries:         3,
					InitialBackoff:     100 * time.Millisecond,
					MaxBackoff:         5 * time.Second,
					S3Endpoint:         testS3Endpoint,
					S3AccessKeyID:      workbenchAccessKey,
					S3SecretAccessKey:  workbenchSecretKey,
				}

				processor, err = logcourier.NewProcessor(ctx, cfg)
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				if processor != nil {
					_ = processor.Close()
				}
				if s3Helper != nil {
					_ = s3Helper.DeleteBucket(ctx, testTargetBucket)
				}
			})


			Describe("Threshold Tests", func() {
				It("should process batch when count threshold is met", func() {
					// Insert 6 logs to exceed count threshold of 5
					timestamp := time.Now()
					for i := 0; i < 6; i++ {
						err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "source-bucket",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-%d", i),
							Action:         "GetObject",
							ObjectKey:      fmt.Sprintf("key-%d", i),
							BytesSent:      1024,
							HttpCode:       200,
						}, testTargetBucket, testTargetPrefix)
						Expect(err).NotTo(HaveOccurred())
					}

					// Run one cycle in background and cancel after processing
					testCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()

					go func() {
						_ = processor.Run(testCtx)
					}()

					// Wait for processing
					time.Sleep(2 * time.Second)

					// Check that log object was uploaded to S3
					objects, mvErr := s3Helper.ListObjects(ctx, testTargetBucket, testTargetPrefix)
					Expect(mvErr).NotTo(HaveOccurred())
					Expect(objects).NotTo(BeEmpty(), "Expected at least one log object in S3")

					// Verify object content
					content, objErr := s3Helper.GetObject(ctx, testTargetBucket, objects[0])
					Expect(objErr).NotTo(HaveOccurred())
					Expect(content).NotTo(BeEmpty())

					contentStr := string(content)
					Expect(strings.Count(contentStr, "\n")).To(BeNumerically(">=", 6), "Expected at least 6 log lines")

					// Verify offset was committed
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
					offset, err := offsetMgr.GetOffset(ctx, "source-bucket", 1)
					Expect(err).NotTo(HaveOccurred())
					Expect(offset.IsZero()).To(BeFalse(), "Expected offset to be set after processing")
				})

				It("should process batch when time threshold is exceeded", func() {
					// Create a processor with a short time threshold (2 seconds)
					cfg := logcourier.Config{
						Logger:             logger,
						ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
						ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
						ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
						ClickHouseDatabase: helper.DatabaseName,
						ClickHouseTimeout:  30 * time.Second,
						CountThreshold:     10,
						TimeThresholdSec:   2,
						DiscoveryInterval:  5 * time.Second,
						NumWorkers:         2,
						MaxRetries:         3,
						InitialBackoff:     100 * time.Millisecond,
						MaxBackoff:         5 * time.Second,
						S3Endpoint:         testS3Endpoint,
						S3AccessKeyID:      workbenchAccessKey,
						S3SecretAccessKey:  workbenchSecretKey,
					}

					timeProcessor, err := logcourier.NewProcessor(ctx, cfg)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = timeProcessor.Close() }()

					// Insert 5 logs (below count threshold of 10)
					eventTimestamp := time.Now()
					for i := 0; i < 5; i++ {
						err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "time-threshold-bucket",
							RaftSessionID:  1,
							Timestamp:      eventTimestamp.Add(time.Duration(i) * time.Second),
							ReqID:          fmt.Sprintf("req-time-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, testTargetBucket, "time-test/")
						Expect(err).NotTo(HaveOccurred())
					}

					// Wait for logs to exceed time threshold (2 seconds + buffer)
					time.Sleep(3 * time.Second)

					// Run processor
					testCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
					defer cancel()

					go func() {
						_ = timeProcessor.Run(testCtx)
					}()

					// Wait for processing
					time.Sleep(2 * time.Second)
					cancel()

					// Verify batch was processed despite low count
					objects, listErr := s3Helper.ListObjects(ctx, testTargetBucket, "time-test/")
					Expect(listErr).NotTo(HaveOccurred())
					Expect(objects).To(HaveLen(1), "Expected time threshold to trigger batch processing despite low count")

					// Verify content
					content, objErr := s3Helper.GetObject(ctx, testTargetBucket, objects[0])
					Expect(objErr).NotTo(HaveOccurred())
					contentStr := string(content)
					Expect(strings.Count(contentStr, "\n")).To(Equal(5), "Expected 5 log lines with newlines")

					// Verify offset was committed
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
					offset, offsetErr := offsetMgr.GetOffset(ctx, "time-threshold-bucket", 1)
					Expect(offsetErr).NotTo(HaveOccurred())
					Expect(offset.IsZero()).To(BeFalse(), "Expected offset to be set after time-based processing")
				})

			})

			Describe("Error Handling", func() {
				It("should not retry when target bucket does not exist", func() {
					// Create real S3 client and wrap it with counting uploader
					s3Config := testutil.GetS3Config()
					s3Client, err := s3.NewClient(ctx, s3Config)
					Expect(err).NotTo(HaveOccurred())

					uploader := s3.NewUploader(s3Client)
					countingUploader := testutil.NewCountingUploader(uploader)

					// Create processor with counting uploader
					cfg := logcourier.Config{
						Logger:             logger,
						ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
								ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHouseDatabase: helper.DatabaseName,
						ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
						ClickHouseTimeout:  30 * time.Second,
						CountThreshold:     5,
						TimeThresholdSec:   60,
						DiscoveryInterval:  5 * time.Second,
						NumWorkers:         2,
						MaxRetries:         3,
						InitialBackoff:     100 * time.Millisecond,
						MaxBackoff:         5 * time.Second,
						S3Uploader:         countingUploader,
					}

					testProcessor, err := logcourier.NewProcessor(ctx, cfg)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = testProcessor.Close() }()

					// Insert logs with non-existent target bucket (NoSuchBucket error)
					timestamp := time.Now()
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "perm-error-bucket",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-perm-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, "nonexistent-bucket-permanent-12345", testTargetPrefix)
						Expect(insertErr).NotTo(HaveOccurred())
					}

					// Process - should fail immediately without retries
					testCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()

					go func() {
						_ = testProcessor.Run(testCtx)
					}()

					// Wait for processing attempt
					time.Sleep(1 * time.Second)
					cancel()

					// Verify only one upload attempt was made
					uploadCount := countingUploader.GetUploadCount()
					Expect(uploadCount).To(Equal(int64(1)), "Should attempt upload exactly once")

					// Verify the upload failed
					failureCount := countingUploader.GetFailureCount()
					Expect(failureCount).To(Equal(int64(1)), "Upload should have failed")

					// Verify offset was not committed
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
					offset, offsetErr := offsetMgr.GetOffset(ctx, "perm-error-bucket", 1)
					Expect(offsetErr).NotTo(HaveOccurred())
					Expect(offset.IsZero()).To(BeTrue(), "Offset should not be committed after permanent error")
				})

				It("should not retry when S3 credentials are invalid", func() {
					// Create S3 client with invalid credentials
					invalidConfig := testutil.GetS3Config()
					invalidConfig.AccessKeyID += "INVALID"
					s3Client, err := s3.NewClient(ctx, invalidConfig)
					Expect(err).NotTo(HaveOccurred())
	
					uploader := s3.NewUploader(s3Client)
					countingUploader := testutil.NewCountingUploader(uploader)
	
					// Create processor with invalid credentials uploader
					cfg := logcourier.Config{
						Logger:             logger,
						ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
						ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
						ClickHouseDatabase: helper.DatabaseName,
						ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
						ClickHouseTimeout:  30 * time.Second,
						CountThreshold:     5,
						TimeThresholdSec:   60,
						DiscoveryInterval:  5 * time.Second,
						NumWorkers:         2,
						MaxRetries:         3,
						InitialBackoff:     100 * time.Millisecond,
						MaxBackoff:         5 * time.Second,
						S3Uploader:         countingUploader,
					}
	
					testProcessor, err := logcourier.NewProcessor(ctx, cfg)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = testProcessor.Close() }()
	
					// Insert logs with valid target bucket (InvalidAccessKeyId error)
					timestamp := time.Now()
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "invalid-creds-bucket",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-creds-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, testTargetBucket, testTargetPrefix)
						Expect(insertErr).NotTo(HaveOccurred())
					}
	
					// Process - should fail immediately without retries
					testCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
	
					go func() {
						_ = testProcessor.Run(testCtx)
					}()
	
					// Wait for processing attempt
					time.Sleep(1 * time.Second)
					cancel()
	
					// Verify only one upload attempt was made
					uploadCount := countingUploader.GetUploadCount()
					Expect(uploadCount).To(Equal(int64(1)), "Should attempt upload exactly once")
	
					// Verify the upload failed
					failureCount := countingUploader.GetFailureCount()
					Expect(failureCount).To(Equal(int64(1)), "Upload should have failed")
	
					// Verify offset was not committed
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
					offset, offsetErr := offsetMgr.GetOffset(ctx, "invalid-creds-bucket", 1)
					Expect(offsetErr).NotTo(HaveOccurred())
					Expect(offset.IsZero()).To(BeTrue(), "Offset should not be committed after permanent error")
				})

				It("should not retry when account lacks S3 write permissions", func() {
					// IAM endpoint
					iamEndpoint := "http://localhost:8600"

					// Create IAM client using workbench credentials
					iamClient := iam.NewFromConfig(aws.Config{
						Region: "us-east-1",
						Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
							return aws.Credentials{
								AccessKeyID:     workbenchAccessKey,
								SecretAccessKey: workbenchSecretKey,
							}, nil
						}),
					}, func(o *iam.Options) {
						o.BaseEndpoint = aws.String(iamEndpoint)
					})

					// Create a test user with restricted permissions
					userName := fmt.Sprintf("test-readonly-user-%d", time.Now().Unix())
					_, err := iamClient.CreateUser(ctx, &iam.CreateUserInput{
						UserName: aws.String(userName),
					})
					Expect(err).NotTo(HaveOccurred())

					defer func() {
						// Cleanup: delete access keys and user
						listKeysOutput, _ := iamClient.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
							UserName: aws.String(userName),
						})
						for _, key := range listKeysOutput.AccessKeyMetadata {
							_, _ = iamClient.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
								UserName:    aws.String(userName),
								AccessKeyId: key.AccessKeyId,
							})
						}
						_, _ = iamClient.DeleteUser(ctx, &iam.DeleteUserInput{
							UserName: aws.String(userName),
						})
					}()

					// Create access keys for the user
					createKeyOutput, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
						UserName: aws.String(userName),
					})
					Expect(err).NotTo(HaveOccurred())
					restrictedAccessKey := *createKeyOutput.AccessKey.AccessKeyId
					restrictedSecretKey := *createKeyOutput.AccessKey.SecretAccessKey

					// Create and attach a deny policy for S3 PutObject
					policyDocument := `{
						"Version": "2012-10-17",
						"Statement": [{
							"Effect": "Deny",
							"Action": "s3:PutObject",
							"Resource": "*"
						}]
					}`

					policyName := fmt.Sprintf("DenyPutObject-%d", time.Now().Unix())
					createPolicyOutput, err := iamClient.CreatePolicy(ctx, &iam.CreatePolicyInput{
						PolicyName:     aws.String(policyName),
						PolicyDocument: aws.String(policyDocument),
					})
					Expect(err).NotTo(HaveOccurred())

					defer func() {
						// Cleanup policy
						_, _ = iamClient.DetachUserPolicy(ctx, &iam.DetachUserPolicyInput{
							UserName:  aws.String(userName),
							PolicyArn: createPolicyOutput.Policy.Arn,
						})
						_, _ = iamClient.DeletePolicy(ctx, &iam.DeletePolicyInput{
							PolicyArn: createPolicyOutput.Policy.Arn,
						})
					}()

					// Attach policy to user
					_, err = iamClient.AttachUserPolicy(ctx, &iam.AttachUserPolicyInput{
						UserName:  aws.String(userName),
						PolicyArn: createPolicyOutput.Policy.Arn,
					})
					Expect(err).NotTo(HaveOccurred())

					// Wait for policy to propagate
					time.Sleep(2 * time.Second)

					// Create S3 client with restricted credentials
					restrictedConfig := s3.Config{
						Endpoint:        testS3Endpoint,
						AccessKeyID:     restrictedAccessKey,
						SecretAccessKey: restrictedSecretKey,
					}
					restrictedS3Client, err := s3.NewClient(ctx, restrictedConfig)
					Expect(err).NotTo(HaveOccurred())

					uploader := s3.NewUploader(restrictedS3Client)
					countingUploader := testutil.NewCountingUploader(uploader)

					// Create processor with restricted uploader
					cfg := logcourier.Config{
						Logger:             logger,
						ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
						ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
						ClickHouseDatabase: helper.DatabaseName,
						ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
						ClickHouseTimeout:  30 * time.Second,
						CountThreshold:     5,
						TimeThresholdSec:   60,
						DiscoveryInterval:  5 * time.Second,
						NumWorkers:         2,
						MaxRetries:         3,
						InitialBackoff:     100 * time.Millisecond,
						MaxBackoff:         5 * time.Second,
						S3Uploader:         countingUploader,
					}

					testProcessor, err := logcourier.NewProcessor(ctx, cfg)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = testProcessor.Close() }()

					// Insert logs with valid target bucket (AccessDenied error)
					timestamp := time.Now()
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "access-denied-bucket",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-denied-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, testTargetBucket, testTargetPrefix)
						Expect(insertErr).NotTo(HaveOccurred())
					}

					// Process - should fail immediately without retries
					testCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()

					go func() {
						_ = testProcessor.Run(testCtx)
					}()

					// Wait for processing attempt
					time.Sleep(1 * time.Second)
					cancel()

					// Verify only one upload attempt was made
					uploadCount := countingUploader.GetUploadCount()
					Expect(uploadCount).To(Equal(int64(1)), "Should attempt upload exactly once")

					// Verify the upload failed
					failureCount := countingUploader.GetFailureCount()
					Expect(failureCount).To(Equal(int64(1)), "Upload should have failed")

					// Verify offset was not committed
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
					offset, offsetErr := offsetMgr.GetOffset(ctx, "access-denied-bucket", 1)
					Expect(offsetErr).NotTo(HaveOccurred())
					Expect(offset.IsZero()).To(BeTrue(), "Offset should not be committed after permanent error")
				})

			})

			Describe("Parallel Processing", func() {
				It("should process multiple buckets in parallel", func() {
					// Insert logs for bucket-1 (just above count threshold of 5)
					timestamp := time.Now()
					for i := 0; i < 6; i++ {
						err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "parallel-bucket-1",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-1-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, testTargetBucket, "bucket1/")
						Expect(err).NotTo(HaveOccurred())
					}

					// Insert logs for bucket-2 (just above count threshold of 5)
					for i := 0; i < 6; i++ {
						err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "parallel-bucket-2",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-2-%d", i),
							Action:         "PutObject",
							HttpCode:       200,
						}, testTargetBucket, "bucket2/")
						Expect(err).NotTo(HaveOccurred())
					}


					testCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()

					go func() {
						_ = processor.Run(testCtx)
					}()

					// Wait for processing
					time.Sleep(3 * time.Second)
					cancel()

					// Verify both buckets were processed
					bucket1Objects, err1 := s3Helper.ListObjects(ctx, testTargetBucket, "bucket1/")
					Expect(err1).NotTo(HaveOccurred())
					Expect(bucket1Objects).NotTo(BeEmpty())

					bucket2Objects, err2 := s3Helper.ListObjects(ctx, testTargetBucket, "bucket2/")
					Expect(err2).NotTo(HaveOccurred())
					Expect(bucket2Objects).NotTo(BeEmpty())
				})

			})

			Describe("Fault Isolation", func() {
				It("should process successful batches when one batch fails", func() {
					// Create real S3 client and wrap with counting uploader
					s3Config := testutil.GetS3Config()
					s3Client, err := s3.NewClient(ctx, s3Config)
					Expect(err).NotTo(HaveOccurred())

					uploader := s3.NewUploader(s3Client)
					countingUploader := testutil.NewCountingUploader(uploader)

					// Create processor with counting uploader
					cfg := logcourier.Config{
						Logger:             logger,
						ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
								ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHouseDatabase: helper.DatabaseName,
						ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
						ClickHouseTimeout:  30 * time.Second,
						CountThreshold:     5, // Match BeforeEach processor threshold
						TimeThresholdSec:   60,
						DiscoveryInterval:  5 * time.Second,
						NumWorkers:         3, // Allow parallel processing
						MaxRetries:         3,
						InitialBackoff:     100 * time.Millisecond,
						MaxBackoff:         5 * time.Second,
						S3Uploader:         countingUploader,
					}

					testProcessor, err := logcourier.NewProcessor(ctx, cfg)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = testProcessor.Close() }()

					timestamp := time.Now()

					// Insert logs for good-bucket-1 (should succeed)
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "good-bucket-1",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-good1-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, testTargetBucket, "good1/")
						Expect(insertErr).NotTo(HaveOccurred())
					}

					// Insert logs for bad-bucket (should fail - nonexistent target)
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "bad-bucket",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-bad-%d", i),
							Action:         "PutObject",
							HttpCode:       200,
						}, "nonexistent-target-bucket-xyz", "bad/")
						Expect(insertErr).NotTo(HaveOccurred())
					}

					// Insert logs for good-bucket-2 (should succeed)
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "good-bucket-2",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-good2-%d", i),
							Action:         "DeleteObject",
							HttpCode:       204,
						}, testTargetBucket, "good2/")
						Expect(insertErr).NotTo(HaveOccurred())
					}

					testCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()

					go func() {
						_ = testProcessor.Run(testCtx)
					}()

					// Wait for processing
					time.Sleep(3 * time.Second)
					cancel()

					// Verify upload attempts: 3 batches attempted (2 good + 1 bad)
					uploadCount := countingUploader.GetUploadCount()
					Expect(uploadCount).To(Equal(int64(3)), "Should attempt upload for all 3 batches")

					// Verify successes: 2 good buckets succeeded
					successCount := countingUploader.GetSuccessCount()
					Expect(successCount).To(Equal(int64(2)), "Should successfully upload 2 good buckets")

					// Verify failures: 1 bad bucket failed
					failureCount := countingUploader.GetFailureCount()
					Expect(failureCount).To(Equal(int64(1)), "Should have 1 failed upload for bad bucket")

					// Verify that both good buckets were processed successfully
					good1Objects, err1 := s3Helper.ListObjects(ctx, testTargetBucket, "good1/")
					Expect(err1).NotTo(HaveOccurred())
					Expect(good1Objects).To(HaveLen(1), "Expected good-bucket-1 to be processed despite bad-bucket failure")

					good2Objects, err2 := s3Helper.ListObjects(ctx, testTargetBucket, "good2/")
					Expect(err2).NotTo(HaveOccurred())
					Expect(good2Objects).To(HaveLen(1), "Expected good-bucket-2 to be processed despite bad-bucket failure")

					// Verify offsets were committed for successful buckets only
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)

					offset1, err := offsetMgr.GetOffset(ctx, "good-bucket-1", 1)
					Expect(err).NotTo(HaveOccurred())
					Expect(offset1.IsZero()).To(BeFalse(), "Expected offset for good-bucket-1")

					offset2, err := offsetMgr.GetOffset(ctx, "good-bucket-2", 1)
					Expect(err).NotTo(HaveOccurred())
					Expect(offset2.IsZero()).To(BeFalse(), "Expected offset for good-bucket-2")

					// Verify offset was not committed for bad bucket
					badOffset, err := offsetMgr.GetOffset(ctx, "bad-bucket", 1)
					Expect(err).NotTo(HaveOccurred())
					Expect(badOffset.IsZero()).To(BeTrue(), "Expected no offset for bad-bucket after permanent error")
				})

			})

			Describe("Offset Management", func() {
				It("should not duplicate uploads when only offset commit retries", func() {
					// Create S3 client with counting uploader
					s3Config := testutil.GetS3Config()
					s3Client, err := s3.NewClient(ctx, s3Config)
					Expect(err).NotTo(HaveOccurred())

					uploader := s3.NewUploader(s3Client)
					countingUploader := testutil.NewCountingUploader(uploader)

					// Create offset manager and wrap it to inject failures
					offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
					failingOffsetMgr := testutil.NewFailingOffsetManager(offsetMgr, 2) // Fail first 2 attempts

					// Create processor with both wrappers
					cfg := logcourier.Config{
						Logger:             logger,
						ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
								ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHouseDatabase: helper.DatabaseName,
						ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
						ClickHouseTimeout:  30 * time.Second,
						CountThreshold:     5,
						TimeThresholdSec:   60,
						DiscoveryInterval:  5 * time.Second,
						NumWorkers:         2,
						MaxRetries:         3,
						InitialBackoff:     100 * time.Millisecond,
						MaxBackoff:         5 * time.Second,
						S3Uploader:         countingUploader,
						OffsetManager:      failingOffsetMgr,
					}

					testProcessor, err := logcourier.NewProcessor(ctx, cfg)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = testProcessor.Close() }()

					// Insert logs that meet count threshold
					timestamp := time.Now()
					for i := 0; i < 6; i++ {
						insertErr := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
							LoggingEnabled: true,
							BucketName:     "offset-retry-bucket",
							RaftSessionID:  1,
							Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
							ReqID:          fmt.Sprintf("req-offset-%d", i),
							Action:         "GetObject",
							HttpCode:       200,
						}, testTargetBucket, "offset-test/")
						Expect(insertErr).NotTo(HaveOccurred())
					}

					testCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()

					go func() {
						_ = testProcessor.Run(testCtx)
					}()

					time.Sleep(3 * time.Second)
					cancel()

					// Verify upload happened only once despite offset commit retries
					uploadCount := countingUploader.GetUploadCount()
					Expect(uploadCount).To(Equal(int64(1)), "Should upload exactly once")

					successCount := countingUploader.GetSuccessCount()
					Expect(successCount).To(Equal(int64(1)), "Upload should succeed")

					// Verify offset commit was retried (failed 2 times, succeeded on 3rd)
					commitCount := failingOffsetMgr.GetCommitCount()
					Expect(commitCount).To(Equal(int64(3)), "Should try offset commit 3 times (2 failures + 1 success)")

					// Verify only one log object was uploaded (no duplication)
					objects, listErr := s3Helper.ListObjects(ctx, testTargetBucket, "offset-test/")
					Expect(listErr).NotTo(HaveOccurred())
					Expect(objects).To(HaveLen(1), "Expected exactly one log object (no duplication on offset retry)")

					// Verify offset was eventually committed
					offset, offsetErr := offsetMgr.GetOffset(ctx, "offset-retry-bucket", 1)
					Expect(offsetErr).NotTo(HaveOccurred())
					Expect(offset.IsZero()).To(BeFalse(), "Offset should be committed after retries")
				})
			})
		})

		Describe("Cycle Failure Behavior", func() {
			var (
				processor *logcourier.Processor
				s3Helper  *testutil.S3TestHelper
			)

			BeforeEach(func() {
				// Set up Viper for config access
				viper.Set("clickhouse.url", os.Getenv("LOG_COURIER_CLICKHOUSE_URL"))
				if viper.GetString("clickhouse.url") == "" {
					viper.Set("clickhouse.url", "localhost:9002")
				}
				viper.Set("s3.endpoint", testS3Endpoint)
				viper.Set("s3.access-key-id", workbenchAccessKey)
				viper.Set("s3.secret-access-key", workbenchSecretKey)

				// Create S3 helper
				var err error
				s3Helper, err = testutil.NewS3TestHelper(ctx)
				Expect(err).NotTo(HaveOccurred())

				// Ensure test bucket exists
				err = s3Helper.CreateBucket(ctx, testTargetBucket)
				Expect(err).NotTo(HaveOccurred())

				cfg := logcourier.Config{
					Logger:             logger,
					ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
						ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHouseDatabase: helper.DatabaseName,
					ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
					ClickHouseTimeout:  30 * time.Second,
					CountThreshold:     5,
					TimeThresholdSec:   60,
					DiscoveryInterval:  1 * time.Second,
					NumWorkers:         2,
					MaxRetries:         1,
					InitialBackoff:     100 * time.Millisecond,
					MaxBackoff:         500 * time.Millisecond,
					S3Endpoint:         testS3Endpoint,
					S3AccessKeyID:      workbenchAccessKey,
					S3SecretAccessKey:  workbenchSecretKey,
				}

				processor, err = logcourier.NewProcessor(ctx, cfg)
				Expect(err).NotTo(HaveOccurred())
			})

			AfterEach(func() {
				if processor != nil {
					_ = processor.Close()
				}
				if s3Helper != nil {
					_ = s3Helper.DeleteBucket(ctx, testTargetBucket)
				}
			})

			It("should not fail processor cycle when all batches have permanent errors", func() {
				// Insert logs for two buckets, both with non-existent target buckets
				// The processor should continue running because permanent errors don't cause processor cycle failure
				timestamp := time.Now()

				// Bucket 1: permanent error (target doesn't exist)
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "bucket-perm-1",
						RaftSessionID:  1,
						Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
						ReqID:          fmt.Sprintf("req-b1-%d", i),
						Action:         "GetObject",
						HttpCode:       200,
					}, "nonexistent-target-bucket-1", testTargetPrefix)
					Expect(err).NotTo(HaveOccurred())
				}

				// Bucket 2: permanent error (target doesn't exist)
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "bucket-perm-2",
						RaftSessionID:  1,
						Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
						ReqID:          fmt.Sprintf("req-b2-%d", i),
						Action:         "GetObject",
						HttpCode:       200,
					}, "nonexistent-target-bucket-2", testTargetPrefix)
					Expect(err).NotTo(HaveOccurred())
				}

				testCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				errChan := make(chan error, 1)
				go func() {
					errChan <- processor.Run(testCtx)
				}()

				// Wait for several cycles to run
				time.Sleep(4 * time.Second)
				cancel()

				runErr := <-errChan

				// Processor should stop with context canceled, not because of consecutive failures
				Expect(runErr).To(MatchError(context.Canceled))
			})

			It("should not fail processor cycle when mixing permanent and transient errors", func() {
				// Close the default processor from BeforeEach
				_ = processor.Close()

				// Create offset manager that will:
				// - Fail attempts 1 and 2 (first cycle exhausts retries)
				// - Succeed on attempt 3 (second cycle succeeds)
				offsetMgr := logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
				failingOffsetMgr := testutil.NewFailingOffsetManager(offsetMgr, 2)

				// Create processor with MaxRetries=1 (2 total attempts per cycle)
				cfg := logcourier.Config{
					Logger:             logger,
					ClickHouseHosts:    logcourier.ConfigSpec.GetStringSlice("clickhouse.url"),
						ClickHouseUsername: logcourier.ConfigSpec.GetString("clickhouse.username"),
					ClickHouseDatabase: helper.DatabaseName,
					ClickHousePassword: logcourier.ConfigSpec.GetString("clickhouse.password"),
					ClickHouseTimeout:  30 * time.Second,
					CountThreshold:     5,
					TimeThresholdSec:   60,
					DiscoveryInterval:  1 * time.Second,
					NumWorkers:         2,
					MaxRetries:         1, // 2 total attempts (0 and 1)
					InitialBackoff:     100 * time.Millisecond,
					MaxBackoff:         500 * time.Millisecond,
					S3Endpoint:         testS3Endpoint,
					S3AccessKeyID:      workbenchAccessKey,
					S3SecretAccessKey:  workbenchSecretKey,
					OffsetManager:      failingOffsetMgr,
				}

				var err error
				processor, err = logcourier.NewProcessor(ctx, cfg)
				Expect(err).NotTo(HaveOccurred())

				// Insert logs for two buckets:
				// - Bucket 1: permanent error (target doesn't exist)
				// - Bucket 2: transient error (offset commit exhausts retries in cycle 1, succeeds in cycle 2)
				timestamp := time.Now()

				// Bucket 1: permanent error
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "bucket-perm-mixed",
						RaftSessionID:  1,
						Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
						ReqID:          fmt.Sprintf("req-perm-%d", i),
						Action:         "GetObject",
						HttpCode:       200,
					}, "nonexistent-bucket-mixed", testTargetPrefix)
					Expect(err).NotTo(HaveOccurred())
				}

				// Bucket 2: transient error that will exhaust retries in first cycle
				for i := 0; i < 6; i++ {
					err := helper.InsertTestLogWithTargetBucket(ctx, testutil.TestLogRecord{
						LoggingEnabled: true,
						BucketName:     "bucket-transient-mixed",
						RaftSessionID:  1,
						Timestamp:      timestamp.Add(time.Duration(i) * time.Minute),
						ReqID:          fmt.Sprintf("req-transient-%d", i),
						Action:         "GetObject",
						HttpCode:       200,
					}, testTargetBucket, testTargetPrefix)
					Expect(err).NotTo(HaveOccurred())
				}

				testCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
				defer cancel()

				errChan := make(chan error, 1)
				go func() {
					errChan <- processor.Run(testCtx)
				}()

				// Wait for multiple cycles:
				// Cycle 1: both buckets fail (1 permanent, 1 transient exhausts retries) -> cycle fails
				// Cycle 2: bucket 1 fails (permanent), bucket 2 succeeds (transient) -> cycle succeeds
				time.Sleep(3 * time.Second)
				cancel()

				runErr := <-errChan

				// Processor should stop with context canceled, not consecutive failures
				Expect(runErr).To(MatchError(context.Canceled))
			})
		})
	})
})
