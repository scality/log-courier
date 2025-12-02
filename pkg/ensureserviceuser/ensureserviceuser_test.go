package ensureserviceuser_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/ensureserviceuser"
)

func TestEnsureServiceUser(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EnsureServiceUser Suite")
}

const (
	testAccessKeyID     = "LSOVSCTL01CME9OETI5A"
	testSecretAccessKey = "6xHQtgUX46WwfsxyhhdatdWqlZj0omlgVSLx4qNV" //nolint:gosec // Test credentials
	testIAMEndpoint     = "http://localhost:8600"
	testRegion          = "us-east-1"
)

// IAMTestHelper provides utilities for testing with real Vault IAM
type IAMTestHelper struct {
	Client       *iam.Client
	iamEndpoint  string
	ServiceNames []string
}

// NewIAMTestHelper creates a new test helper with real IAM client
func NewIAMTestHelper(ctx context.Context) (*IAMTestHelper, error) {
	if err := os.Setenv("AWS_ACCESS_KEY_ID", testAccessKeyID); err != nil {
		return nil, fmt.Errorf("failed to set AWS_ACCESS_KEY_ID: %w", err)
	}
	if err := os.Setenv("AWS_SECRET_ACCESS_KEY", testSecretAccessKey); err != nil {
		return nil, fmt.Errorf("failed to set AWS_SECRET_ACCESS_KEY: %w", err)
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(testRegion),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	iamClient := iam.NewFromConfig(cfg, func(o *iam.Options) {
		o.BaseEndpoint = aws.String(testIAMEndpoint)
	})

	return &IAMTestHelper{
		Client:       iamClient,
		ServiceNames: []string{},
		iamEndpoint:  testIAMEndpoint,
	}, nil
}

// GenerateServiceName creates a unique service name for test isolation
func (h *IAMTestHelper) GenerateServiceName(prefix string) string {
	serviceName := fmt.Sprintf("test-%s-%d", prefix, time.Now().UnixNano())
	h.ServiceNames = append(h.ServiceNames, serviceName)
	return serviceName
}

// Cleanup removes all test users, their policies, and access keys
func (h *IAMTestHelper) Cleanup(ctx context.Context) error {
	for _, serviceName := range h.ServiceNames {
		listOutput, err := h.Client.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
			UserName: aws.String(serviceName),
		})
		if err != nil {
			var noSuchEntity *types.NoSuchEntityException
			if errors.As(err, &noSuchEntity) {
				continue
			}
			// Log error but continue cleanup
			fmt.Fprintf(os.Stderr, "failed to list access keys for %s: %v\n", serviceName, err)
		} else {
			for _, key := range listOutput.AccessKeyMetadata {
				_, err = h.Client.DeleteAccessKey(ctx, &iam.DeleteAccessKeyInput{
					UserName:    aws.String(serviceName),
					AccessKeyId: key.AccessKeyId,
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "failed to delete access key %s: %v\n", *key.AccessKeyId, err)
				}
			}
		}

		_, err = h.Client.DeleteUserPolicy(ctx, &iam.DeleteUserPolicyInput{
			UserName:   aws.String(serviceName),
			PolicyName: aws.String(serviceName),
		})
		if err != nil {
			var noSuchEntity *types.NoSuchEntityException
			if !errors.As(err, &noSuchEntity) {
				fmt.Fprintf(os.Stderr, "failed to delete policy for %s: %v\n", serviceName, err)
			}
		}

		_, err = h.Client.DeleteUser(ctx, &iam.DeleteUserInput{
			UserName: aws.String(serviceName),
		})
		if err != nil {
			var noSuchEntity *types.NoSuchEntityException
			if !errors.As(err, &noSuchEntity) {
				fmt.Fprintf(os.Stderr, "failed to delete user %s: %v\n", serviceName, err)
			}
		}
	}

	return nil
}

var _ = Describe("Apply", func() {
	var (
		ctx    context.Context
		helper *IAMTestHelper
	)

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		helper, err = NewIAMTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(helper).NotTo(BeNil())
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.Cleanup(ctx)
		}
	})

	Describe("User creation", func() {
		It("should create user when it doesn't exist", func() {
			serviceName := helper.GenerateServiceName("create")

			result, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.AccessKeyId).NotTo(BeEmpty())
			Expect(result.SecretAccessKey).NotTo(BeNil())
			Expect(*result.SecretAccessKey).NotTo(BeEmpty())

			getUserOutput, err := helper.Client.GetUser(ctx, &iam.GetUserInput{
				UserName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(getUserOutput.User.Path).NotTo(BeNil())
			Expect(*getUserOutput.User.Path).To(Equal("/scality-internal/"))
		})

		It("should be idempotent when user already exists", func() {
			serviceName := helper.GenerateServiceName("idempotent")

			// First call creates user
			result1, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result1.SecretAccessKey).NotTo(BeNil())

			// Second call should succeed and return existing access key (no secret)
			result2, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result2.AccessKeyId).To(Equal(result1.AccessKeyId))
			Expect(result2.SecretAccessKey).To(BeNil())
		})

		It("should reject existing user with wrong path", func() {
			serviceName := helper.GenerateServiceName("wrongpath")

			// Create user with wrong path manually
			_, err := helper.Client.CreateUser(ctx, &iam.CreateUserInput{
				UserName: aws.String(serviceName),
				Path:     aws.String("/different-path/"),
			})
			Expect(err).NotTo(HaveOccurred())

			// Apply should fail with path conflict
			_, err = ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("conflicting path"))
		})
	})

	Describe("Policy attachment", func() {
		It("should attach policy to user", func() {
			serviceName := helper.GenerateServiceName("policy")

			result, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			policyOutput, err := helper.Client.GetUserPolicy(ctx, &iam.GetUserPolicyInput{
				UserName:   aws.String(serviceName),
				PolicyName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(policyOutput.PolicyDocument).NotTo(BeNil())

			decodedPolicy, err := url.QueryUnescape(*policyOutput.PolicyDocument)
			Expect(err).NotTo(HaveOccurred())
			Expect(decodedPolicy).To(ContainSubstring("s3:PutObject"))
			Expect(decodedPolicy).To(ContainSubstring("arn:aws:s3:::*/*"))
		})

		It("should be idempotent when policy already exists", func() {
			serviceName := helper.GenerateServiceName("policy-idempotent")

			_, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())

			_, err = ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("Access key management", func() {
		It("should create access key for new user", func() {
			serviceName := helper.GenerateServiceName("newkey")

			result, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.AccessKeyId).NotTo(BeEmpty())
			Expect(result.SecretAccessKey).NotTo(BeNil())
			Expect(*result.SecretAccessKey).NotTo(BeEmpty())

			listOutput, err := helper.Client.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
				UserName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(listOutput.AccessKeyMetadata).To(HaveLen(1))
			Expect(*listOutput.AccessKeyMetadata[0].AccessKeyId).To(Equal(result.AccessKeyId))
		})

		It("should return existing access key without secret", func() {
			serviceName := helper.GenerateServiceName("existingkey")

			// First call creates key
			result1, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result1.SecretAccessKey).NotTo(BeNil())

			// Second call returns same key without secret
			result2, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())
			Expect(result2.AccessKeyId).To(Equal(result1.AccessKeyId))
			Expect(result2.SecretAccessKey).To(BeNil())
		})

		It("should not create duplicate keys", func() {
			serviceName := helper.GenerateServiceName("nodup")

			_, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())

			_, err = ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())

			listOutput, err := helper.Client.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
				UserName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(listOutput.AccessKeyMetadata).To(HaveLen(1))
		})
	})

	Describe("Complete workflow", func() {
		It("should create complete service user setup", func() {
			serviceName := helper.GenerateServiceName("complete")

			result, err := ensureserviceuser.Apply(ctx, helper.Client, serviceName)
			Expect(err).NotTo(HaveOccurred())

			getUserOutput, err := helper.Client.GetUser(ctx, &iam.GetUserInput{
				UserName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(*getUserOutput.User.Path).To(Equal("/scality-internal/"))

			policyOutput, err := helper.Client.GetUserPolicy(ctx, &iam.GetUserPolicyInput{
				UserName:   aws.String(serviceName),
				PolicyName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())

			decodedPolicy, err := url.QueryUnescape(*policyOutput.PolicyDocument)
			Expect(err).NotTo(HaveOccurred())
			Expect(decodedPolicy).To(ContainSubstring("s3:PutObject"))

			listOutput, err := helper.Client.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
				UserName: aws.String(serviceName),
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(listOutput.AccessKeyMetadata).To(HaveLen(1))
			Expect(*listOutput.AccessKeyMetadata[0].AccessKeyId).To(Equal(result.AccessKeyId))
		})
	})

	Describe("Error handling", func() {
		It("should handle empty service name", func() {
			_, err := ensureserviceuser.Apply(ctx, helper.Client, "")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("service name cannot be empty"))
		})
	})
})
