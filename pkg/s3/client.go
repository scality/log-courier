package s3

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultRegion = "us-east-1"

	// Default HTTP client timeouts
	defaultDialTimeout            = 10 * time.Second  // Time to establish connection
	defaultResponseHeaderTimeout  = 30 * time.Second  // Time to receive response headers
	defaultIdleConnTimeout        = 90 * time.Second  // Time to keep idle connections
	defaultTLSHandshakeTimeout    = 10 * time.Second  // Time for TLS handshake
	defaultExpectContinueTimeout  = 1 * time.Second   // Time waiting for 100-Continue
)

// Client wraps S3 client
type Client struct {
	s3Client *s3.Client
}

// Config holds S3 client configuration
type Config struct {
	Endpoint         string
	AccessKeyID      string
	SecretAccessKey  string
	MaxRetryAttempts int
	MaxBackoffDelay  time.Duration
}

// NewClient creates a new S3 client
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("access key ID and secret access key are required")
	}

	var optFns []func(*config.LoadOptions) error

	// Create HTTP client with timeouts to prevent indefinite hangs
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: defaultDialTimeout,
			}).DialContext,
			ResponseHeaderTimeout: defaultResponseHeaderTimeout,
			IdleConnTimeout:       defaultIdleConnTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
			ExpectContinueTimeout: defaultExpectContinueTimeout,
		},
	}

	// Set HTTP client, region, and credentials
	optFns = append(optFns,
		config.WithHTTPClient(httpClient),
		config.WithRegion(defaultRegion),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				"",
			),
		),
	)

	// Set retry configuration if non-zero values provided
	if cfg.MaxRetryAttempts > 0 || cfg.MaxBackoffDelay > 0 {
		optFns = append(optFns, config.WithRetryer(func() aws.Retryer {
			retryer := retry.NewStandard()
			var result aws.Retryer = retryer
			if cfg.MaxRetryAttempts > 0 {
				result = retry.AddWithMaxAttempts(result, cfg.MaxRetryAttempts)
			}
			if cfg.MaxBackoffDelay > 0 {
				result = retry.AddWithMaxBackoffDelay(result, cfg.MaxBackoffDelay)
			}
			return result
		}))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3ClientOpts := []func(*s3.Options){}

	// Set endpoint
	if cfg.Endpoint != "" {
		s3ClientOpts = append(s3ClientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // Required for non-AWS S3-compatible services
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3ClientOpts...)

	return &Client{s3Client: s3Client}, nil
}
