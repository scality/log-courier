package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// replicationRoleARN is provisioned by workbench's setup-vault via
	// vault accountSeeds. The account ID is pinned to 123456789012 in
	// templates/vault/create-management-account.sh.
	replicationRoleARN = "arn:aws:iam::123456789012:role/scality-internal/replication-role"

	// Test buckets must be owned by the same account as the replication role
	// (123456789012 / testaccount). Using management-account credentials would
	// create buckets in account 000000000000, breaking cross-account auth when
	// backbeat assumes the role and tries to call S3.
	testaccountAccessKeyID     = "WBTKACCESSI9O3YKIRQ0"
	testaccountSecretAccessKey = "ICxmNTBbOqijy4rMq/MOP1EPlTMqfsEBLjROcAbN" //nolint:gosec // Test credentials

	replicationTimeout = 60 * time.Second
	replicationPoll    = 2 * time.Second
)

var _ = Describe("Cross-region replication", func() {
	var (
		sourceBucket    string
		destBucket      string
		testaccountClnt *s3.Client
	)

	BeforeEach(func(ctx context.Context) {
		testaccountClnt = newS3ClientWithCredentials(testaccountAccessKeyID, testaccountSecretAccessKey, "")

		timestamp := time.Now().UnixNano()
		sourceBucket = fmt.Sprintf("e2e-crr-src-%d", timestamp)
		destBucket = fmt.Sprintf("e2e-crr-dst-%d", timestamp)

		for _, b := range []string{sourceBucket, destBucket} {
			Expect(createBucketWithRetry(testaccountClnt, b)).To(Succeed())

			_, err := testaccountClnt.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
				Bucket: aws.String(b),
				VersioningConfiguration: &types.VersioningConfiguration{
					Status: types.BucketVersioningStatusEnabled,
				},
			})
			Expect(err).NotTo(HaveOccurred(), "enable versioning on %s", b)
		}

		_, err := testaccountClnt.PutBucketReplication(ctx, &s3.PutBucketReplicationInput{
			Bucket: aws.String(sourceBucket),
			ReplicationConfiguration: &types.ReplicationConfiguration{
				Role: aws.String(fmt.Sprintf("%s,%s", replicationRoleARN, replicationRoleARN)),
				Rules: []types.ReplicationRule{
					{
						ID:     aws.String("e2e-crr"),
						Status: types.ReplicationRuleStatusEnabled,
						Filter: &types.ReplicationRuleFilter{Prefix: aws.String("")},
						Destination: &types.Destination{
							Bucket:       aws.String("arn:aws:s3:::" + destBucket),
							StorageClass: types.StorageClass("sf"),
						},
					},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "configure bucket replication")
	})

	AfterEach(func(ctx context.Context) {
		emptyVersionedBucket(ctx, testaccountClnt, sourceBucket)
		emptyVersionedBucket(ctx, testaccountClnt, destBucket)
		_, _ = testaccountClnt.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(sourceBucket)})
		_, _ = testaccountClnt.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(destBucket)})
	})

	It("replicates an object from source to destination", func(ctx context.Context) {
		key := "crr-object.txt"
		content := []byte("data to be replicated")

		_, err := testaccountClnt.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT to source")

		Eventually(func() error {
			_, headErr := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(destBucket),
				Key:    aws.String(key),
			})
			return headErr
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(Succeed(), "replica should appear in destination bucket")
	})
})

func emptyVersionedBucket(ctx context.Context, client *s3.Client, bucket string) {
	paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucket),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return
		}
		var ids []types.ObjectIdentifier
		for i := range page.Versions {
			v := &page.Versions[i]
			ids = append(ids, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for i := range page.DeleteMarkers {
			dm := &page.DeleteMarkers[i]
			ids = append(ids, types.ObjectIdentifier{Key: dm.Key, VersionId: dm.VersionId})
		}
		if len(ids) == 0 {
			continue
		}
		_, _ = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: ids},
		})
	}
}
