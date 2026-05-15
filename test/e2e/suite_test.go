package e2e_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
)

// === Test account model ===
//
// Three logically distinct accounts appear in these tests:
//
//  1. test-client account  — sharedS3Client authenticates as this. Owns
//     IAM users created by requester_field_test for Requester-field
//     assertions.
//     (testAccountID)
//
//  2. log-courier account  — the account log-courier authenticates as
//     when delivering log objects. CRR tests add a cross-account
//     PutObject grant for this principal on log target buckets.
//     (logCourierAccountID)
//
//  3. replication-role account — owns scality-internal/replication-role
//     and the CRR buckets that reference it (vault requires the role
//     and the buckets to be co-located). CRR tests build a dedicated
//     S3 client with these credentials.
//     (testaccountAccessKeyID/Secret)
//
// In workbench, (1) and (2) resolve to the management account 000000000000
// and (3) to the seeded testaccount 123456789012. In Integration, (1)
// and (3) both resolve to account1; (2) keeps the default value and goes
// unused — log-courier runs under account1 there, so the cross-account
// grant on the log target bucket is a no-op.
const (
	testRegion               = "us-east-1"
	testS3Endpoint           = "http://127.0.0.1:8000"
	testIAMEndpoint          = "http://127.0.0.1:8600"
	testSTSEndpoint          = "http://127.0.0.1:8800"
	testAccessKeyID          = "LSOVSCTL01CME9OETI5A"
	testSecretAccessKey      = "6xHQtgUX46WwfsxyhhdatdWqlZj0omlgVSLx4qNV" //nolint:gosec // Test credentials
	defaultTestAccountID     = "000000000000"
	defaultLogCourierAccount = "000000000000"

	defaultTestaccountAccessKeyID     = "WBTKACCESSI9O3YKIRQ0"
	defaultTestaccountSecretAccessKey = "ICxmNTBbOqijy4rMq/MOP1EPlTMqfsEBLjROcAbN" //nolint:gosec // Test credentials
)

var (
	sharedS3Client *s3.Client //nolint:gochecknoglobals // Shared test client initialized in BeforeSuite

	// Test-client account; see "Test account model" above.
	testAccountID = envOrDefault("E2E_ACCOUNT_ID", defaultTestAccountID) //nolint:gochecknoglobals // Env-driven test fixture initialized at package load

	// Log-courier account; see "Test account model" above.
	logCourierAccountID = envOrDefault("E2E_LOG_COURIER_ACCOUNT_ID", defaultLogCourierAccount) //nolint:gochecknoglobals // Env-driven test fixture initialized at package load

	// Replication-role account credentials; see "Test account model" above.
	testaccountAccessKeyID     = envOrDefault("E2E_TESTACCOUNT_ACCESS_KEY_ID", defaultTestaccountAccessKeyID)         //nolint:gochecknoglobals // Env-driven test fixture initialized at package load
	testaccountSecretAccessKey = envOrDefault("E2E_TESTACCOUNT_SECRET_ACCESS_KEY", defaultTestaccountSecretAccessKey) //nolint:gochecknoglobals,gosec // Env-driven test fixture initialized at package load
)

var _ = BeforeSuite(func() {
	// Load configuration
	logcourier.ConfigSpec.Reset()
	err := logcourier.ConfigSpec.LoadConfiguration("", "", nil)
	Expect(err).NotTo(HaveOccurred())

	// Get S3 credentials for test operations.
	// Check environment variables first, fall back to defaults.
	endpoint := envOrDefault("E2E_S3_ENDPOINT", testS3Endpoint)
	accessKey := envOrDefault("E2E_S3_ACCESS_KEY_ID", testAccessKeyID)
	secretKey := envOrDefault("E2E_S3_SECRET_ACCESS_KEY", testSecretAccessKey)

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
