package e2e_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	testS3FrontendEndpoint = "https://127.0.0.1:443"
)

func newTLSS3Client(accessKeyID, secretAccessKey, endpoint string) *s3.Client {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // self-signed cert in test environment
		},
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return s3.NewFromConfig(aws.Config{
		Region: testRegion,
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			}, nil
		}),
		HTTPClient: &http.Client{Transport: transport},
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

var _ = Describe("TLS fields in access logs", func() {
	var testCtx *E2ETestContext

	BeforeEach(func() {
		testCtx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(testCtx)
	})

	It("logs CipherSuite and TLSVersion for requests through the S3 frontend", func(ctx context.Context) {
		endpoint := os.Getenv("E2E_S3_FRONTEND_ENDPOINT")
		if endpoint == "" {
			endpoint = testS3FrontendEndpoint
		}

		accessKey := os.Getenv("E2E_S3_ACCESS_KEY_ID")
		if accessKey == "" {
			accessKey = testAccessKeyID
		}

		secretKey := os.Getenv("E2E_S3_SECRET_ACCESS_KEY")
		if secretKey == "" {
			secretKey = testSecretAccessKey
		}

		tlsClient := newTLSS3Client(accessKey, secretKey, endpoint)

		testKey := "tls-test-object.txt"
		testContent := []byte("test data for TLS field verification")

		_, err := tlsClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT through S3 frontend should succeed")

		_, err = tlsClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(testCtx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET through S3 frontend should succeed")

		logs := testCtx.VerifyLogs(
			testCtx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			testCtx.ObjectOp("REST.GET.OBJECT", testKey, 200).WithBytesSent(int64(len(testContent))).WithObjectSize(int64(len(testContent))),
		)

		for _, log := range logs {
			Expect(log.CipherSuite).NotTo(Equal("-"),
				"CipherSuite should be populated for TLS requests (got '-')")
			Expect(log.CipherSuite).NotTo(BeEmpty(),
				"CipherSuite should not be empty for TLS requests")

			Expect(log.TLSVersion).NotTo(Equal("-"),
				"TLSVersion should be populated for TLS requests (got '-')")
			Expect(log.TLSVersion).NotTo(BeEmpty(),
				"TLSVersion should not be empty for TLS requests")
			Expect(log.TLSVersion).To(MatchRegexp(`^TLSv1\.[23]$`),
				"TLSVersion should be TLSv1.2 or TLSv1.3")
		}
	})
})
