package testutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/spf13/viper"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/s3"
)

const (
	defaultTestRegion = "us-east-1"
)

// S3TestHelper provides utilities for testing with S3
type S3TestHelper struct {
	client   *awss3.Client
	endpoint string
}

// NewS3TestHelper creates a new S3 test helper
func NewS3TestHelper(ctx context.Context) (*S3TestHelper, error) {
	endpoint := logcourier.ConfigSpec.GetString("s3.endpoint")
	accessKey := logcourier.ConfigSpec.GetString("s3.access-key-id")
	secretKey := logcourier.ConfigSpec.GetString("s3.secret-access-key")

	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3 credentials not configured")
	}

	s3Client := awss3.NewFromConfig(aws.Config{
		Region: defaultTestRegion,
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKey,
				SecretAccessKey: secretKey,
			}, nil
		}),
	}, func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})

	return &S3TestHelper{
		client:   s3Client,
		endpoint: endpoint,
	}, nil
}

// CreateBucket creates a test bucket
func (h *S3TestHelper) CreateBucket(ctx context.Context, bucketName string) error {
	_, err := h.client.CreateBucket(ctx, &awss3.CreateBucketInput{
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

// DeleteBucket deletes a test bucket and all its objects
func (h *S3TestHelper) DeleteBucket(ctx context.Context, bucketName string) error {
	// List and delete all objects first
	listOutput, err := h.client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		var noSuchBucket *types.NoSuchBucket
		if errors.As(err, &noSuchBucket) {
			return nil
		}
		return fmt.Errorf("failed to list objects: %w", err)
	}

	// Delete objects in batch
	if len(listOutput.Contents) > 0 {
		objectsToDelete := make([]types.ObjectIdentifier, len(listOutput.Contents))
		for i, obj := range listOutput.Contents {
			objectsToDelete[i] = types.ObjectIdentifier{
				Key: obj.Key,
			}
		}

		_, err = h.client.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &types.Delete{
				Objects: objectsToDelete,
			},
		})
		if err != nil {
			return fmt.Errorf("failed to delete objects: %w", err)
		}
	}

	// Delete bucket
	_, err = h.client.DeleteBucket(ctx, &awss3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		var noSuchBucket *types.NoSuchBucket
		if errors.As(err, &noSuchBucket) {
			return nil
		}
		return fmt.Errorf("failed to delete bucket: %w", err)
	}

	return nil
}

// GetObject retrieves an object from S3
func (h *S3TestHelper) GetObject(ctx context.Context, bucketName, key string) ([]byte, error) {
	output, err := h.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer func() { _ = output.Body.Close() }()

	content, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read object content: %w", err)
	}

	return content, nil
}

// ObjectExists checks if an object exists in S3
func (h *S3TestHelper) ObjectExists(ctx context.Context, bucketName, key string) (bool, error) {
	_, err := h.client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence: %w", err)
	}
	return true, nil
}

// ListObjects lists all objects in a bucket with optional prefix
func (h *S3TestHelper) ListObjects(ctx context.Context, bucketName, prefix string) ([]string, error) {
	input := &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	output, err := h.client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	keys := make([]string, 0, len(output.Contents))
	for _, obj := range output.Contents {
		keys = append(keys, *obj.Key)
	}

	return keys, nil
}

// GetS3Config returns S3 configuration
func GetS3Config() s3.Config {
	for key, spec := range logcourier.ConfigSpec {
		viper.SetDefault(key, spec.DefaultValue)
		if spec.EnvVar != "" {
			_ = viper.BindEnv(key, spec.EnvVar)
		}
	}

	return s3.Config{
		Endpoint:        logcourier.ConfigSpec.GetString("s3.endpoint"),
		AccessKeyID:     logcourier.ConfigSpec.GetString("s3.access-key-id"),
		SecretAccessKey: logcourier.ConfigSpec.GetString("s3.secret-access-key"),
	}
}

// CountingUploader wraps an S3 uploader and counts upload attempts
type CountingUploader struct {
	uploader     s3.UploaderInterface
	uploadCount  atomic.Int64
	successCount atomic.Int64
	failureCount atomic.Int64
}

// NewCountingUploader creates a new counting uploader wrapper
func NewCountingUploader(uploader s3.UploaderInterface) *CountingUploader {
	return &CountingUploader{
		uploader: uploader,
	}
}

// Upload wraps the underlying uploader's Upload method and counts attempts
func (c *CountingUploader) Upload(ctx context.Context, bucket, key string, content []byte) error {
	c.uploadCount.Add(1)
	err := c.uploader.Upload(ctx, bucket, key, content)
	if err != nil {
		c.failureCount.Add(1)
		return err
	}
	c.successCount.Add(1)
	return nil
}

// GetUploadCount returns the total number of upload attempts
func (c *CountingUploader) GetUploadCount() int64 {
	return c.uploadCount.Load()
}

// GetSuccessCount returns the number of successful uploads
func (c *CountingUploader) GetSuccessCount() int64 {
	return c.successCount.Load()
}

// GetFailureCount returns the number of failed uploads
func (c *CountingUploader) GetFailureCount() int64 {
	return c.failureCount.Load()
}
