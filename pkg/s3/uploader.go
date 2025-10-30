package s3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Uploader uploads log objects to S3
type Uploader struct {
	client *Client
}

// NewUploader creates a new uploader
func NewUploader(client *Client) *Uploader {
	return &Uploader{client: client}
}

// Upload uploads a log object to the specified bucket.
// Retries are handled automatically by the SDK client based on its retry configuration.
func (u *Uploader) Upload(ctx context.Context, bucket, key string, content []byte) error {
	_, err := u.client.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("text/plain"),
	})

	if err != nil {
		return fmt.Errorf("failed to upload to S3: bucket=%s, key=%s: %w", bucket, key, err)
	}

	return nil
}
