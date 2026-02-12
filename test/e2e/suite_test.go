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
	testIAMEndpoint     = "http://127.0.0.1:8600"
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

	// Get S3 credentials for test operations
	// Check environment variables first, fall back to defaults
	endpoint := os.Getenv("E2E_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = testS3Endpoint
	}

	accessKey := os.Getenv("E2E_S3_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = testAccessKeyID
	}

	secretKey := os.Getenv("E2E_S3_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = testSecretAccessKey
	}

	// Create shared S3 client for test operations
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
