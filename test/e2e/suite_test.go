package e2e_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
)

const (
	testRegion          = "us-east-1"
	testS3Endpoint      = "http://127.0.0.1:8000"
	testAccessKeyID     = "LSOVSCTL01CME9OETI5A"
	testSecretAccessKey = "6xHQtgUX46WwfsxyhhdatdWqlZj0omlgVSLx4qNV" //nolint:gosec // Test credentials
)

var (
	sharedS3Client *s3.Client //nolint:gochecknoglobals // Shared test client initialized in BeforeSuite
)

var _ = BeforeSuite(func() {
	// Load configuration
	logcourier.ConfigSpec.Reset()
	err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
	Expect(err).NotTo(HaveOccurred())

	// Override S3 config for E2E tests
	// Check environment variables first, fall back to defaults
	endpoint := os.Getenv("LOG_COURIER_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = testS3Endpoint
	}

	accessKey := os.Getenv("LOG_COURIER_S3_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = testAccessKeyID
	}

	secretKey := os.Getenv("LOG_COURIER_S3_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = testSecretAccessKey
	}

	logcourier.ConfigSpec.Set("s3.endpoint", endpoint)
	logcourier.ConfigSpec.Set("s3.access-key-id", accessKey)
	logcourier.ConfigSpec.Set("s3.secret-access-key", secretKey)

	// Create shared S3 client
	sharedS3Client = s3.NewFromConfig(aws.Config{
		Region: testRegion,
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKey,
				SecretAccessKey: secretKey,
			}, nil
		}),
	}, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
})

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}
