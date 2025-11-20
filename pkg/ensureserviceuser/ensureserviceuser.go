package ensureserviceuser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
)

const (
	servicePath = "/scality-internal/"
)

// Apply ensures a service user exists in IAM with appropriate S3 permissions.
// It creates the user, attaches the policy, and ensures an access key exists.
// Returns the access key information.
func Apply(ctx context.Context, iamClient *iam.Client, serviceName string) (*Result, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	if err := ensureUser(ctx, iamClient, serviceName); err != nil {
		return nil, fmt.Errorf("failed to ensure user: %w", err)
	}

	if err := ensurePolicy(ctx, iamClient, serviceName); err != nil {
		return nil, fmt.Errorf("failed to ensure policy: %w", err)
	}

	result, err := ensureAccessKey(ctx, iamClient, serviceName)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure access key: %w", err)
	}

	return result, nil
}

// ensureUser creates the user if it doesn't exist, or validates the existing user's path.
func ensureUser(ctx context.Context, iamClient *iam.Client, serviceName string) error {
	getUserOutput, err := iamClient.GetUser(ctx, &iam.GetUserInput{
		UserName: aws.String(serviceName),
	})

	if err == nil {
		if getUserOutput.User.Path != nil && *getUserOutput.User.Path != servicePath {
			return fmt.Errorf("user already exists with conflicting path: %s", *getUserOutput.User.Path)
		}
		return nil
	}

	var noSuchEntity *types.NoSuchEntityException
	if !errors.As(err, &noSuchEntity) {
		return fmt.Errorf("get user failed: %w", err)
	}

	_, err = iamClient.CreateUser(ctx, &iam.CreateUserInput{
		UserName: aws.String(serviceName),
		Path:     aws.String(servicePath),
	})
	if err != nil {
		return fmt.Errorf("create user failed: %w", err)
	}

	return nil
}

// ensurePolicy attaches the S3 PutObject policy to the user.
func ensurePolicy(ctx context.Context, iamClient *iam.Client, serviceName string) error {
	policyDoc := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect":   "Allow",
				"Action":   "s3:PutObject",
				"Resource": "arn:aws:s3:::*/*",
			},
		},
	}

	policyJSON, err := json.Marshal(policyDoc)
	if err != nil {
		return fmt.Errorf("marshal policy document failed: %w", err)
	}

	_, err = iamClient.PutUserPolicy(ctx, &iam.PutUserPolicyInput{
		UserName:       aws.String(serviceName),
		PolicyName:     aws.String(serviceName),
		PolicyDocument: aws.String(string(policyJSON)),
	})
	if err != nil {
		return fmt.Errorf("put user policy failed: %w", err)
	}

	return nil
}

// ensureAccessKey creates an access key if none exists, or returns the existing key ID.
func ensureAccessKey(ctx context.Context, iamClient *iam.Client, serviceName string) (*Result, error) {
	listOutput, err := iamClient.ListAccessKeys(ctx, &iam.ListAccessKeysInput{
		UserName: aws.String(serviceName),
	})
	if err != nil {
		return nil, fmt.Errorf("list access keys failed: %w", err)
	}

	if len(listOutput.AccessKeyMetadata) > 0 {
		return &Result{
			AccessKeyId:     *listOutput.AccessKeyMetadata[0].AccessKeyId,
			SecretAccessKey: nil,
		}, nil
	}

	createOutput, err := iamClient.CreateAccessKey(ctx, &iam.CreateAccessKeyInput{
		UserName: aws.String(serviceName),
	})
	if err != nil {
		return nil, fmt.Errorf("create access key failed: %w", err)
	}

	return &Result{
		AccessKeyId:     *createOutput.AccessKey.AccessKeyId,
		SecretAccessKey: createOutput.AccessKey.SecretAccessKey,
	}, nil
}
