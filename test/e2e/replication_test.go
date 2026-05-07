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

	replicationTimeout = 30 * time.Second
	replicationPoll    = 2 * time.Second

	// logCourierAccountRootARN is the principal log-courier writes log
	// objects as. log-courier authenticates with the management-account
	// root credentials (testAccountID), which is a different account
	// than the testaccount that owns the replication buckets, so log
	// delivery to the log target bucket needs a cross-account grant.
	logCourierAccountRootARN = "arn:aws:iam::" + testAccountID + ":root"

	replicaLogPrefix = "dst/"
)

var _ = Describe("Cross-region replication", func() {
	var (
		sourceBucket    string
		destBucket      string
		logBucket       string
		testaccountClnt *s3.Client
		testStartTime   time.Time
	)

	BeforeEach(func(ctx context.Context) {
		testaccountClnt = newS3ClientWithCredentials(testaccountAccessKeyID, testaccountSecretAccessKey, "")
		timestamp := time.Now().UnixNano()
		sourceBucket = fmt.Sprintf("e2e-crr-src-%d", timestamp)
		destBucket = fmt.Sprintf("e2e-crr-dst-%d", timestamp)
		logBucket = fmt.Sprintf("e2e-crr-log-%d", timestamp)
		testStartTime = time.Now()
		setupReplicationBuckets(ctx, testaccountClnt, sourceBucket, destBucket, logBucket)
	})

	AfterEach(func(ctx context.Context) {
		cleanupReplicationBuckets(ctx, testaccountClnt, sourceBucket, destBucket, logBucket)
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

		Eventually(func(g Gomega) types.ReplicationStatus {
			out, headErr := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    aws.String(key),
			})
			g.Expect(headErr).NotTo(HaveOccurred())
			return out.ReplicationStatus
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(Equal(types.ReplicationStatusCompleted),
				"source object replication status should reach COMPLETED")

		destOut, err := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(destBucket),
			Key:    aws.String(key),
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD on replica")
		Expect(destOut.ReplicationStatus).To(Equal(types.ReplicationStatusReplica),
			"destination object replication status should be REPLICA")

		// Per CLDSRV-899, backbeat replication writes (the putData
		// route) appear in destination-bucket access logs as a regular
		// REST.PUT.OBJECT, with HTTP-layer fields blanked and the
		// requestURI synthesized to match AWS's log shape.
		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaPutEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.PUT.OBJECT entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/replication-role"),
			"replica entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf("PUT /%s/%s HTTP/1.1", destBucket, key)),
			"replica entry's requestURI should be synthesized to a normal S3 PUT")
	})

	It("logs delete-marker replication on destination as REST.DELETE.OBJECT", func(ctx context.Context) {
		key := "crr-deletemarker.txt"
		content := []byte("data to be deleted")

		_, err := testaccountClnt.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT to source")

		Eventually(func(g Gomega) types.ReplicationStatus {
			out, headErr := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    aws.String(key),
			})
			g.Expect(headErr).NotTo(HaveOccurred())
			return out.ReplicationStatus
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(Equal(types.ReplicationStatusCompleted),
				"source object replication status should reach COMPLETED before delete")

		// Issue a versioned delete on source. On a versioned bucket this
		// creates a delete marker, which backbeat then replicates to the
		// destination as a metadata-only put.
		_, err = testaccountClnt.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE on source")

		Eventually(func(g Gomega) bool {
			out, listErr := testaccountClnt.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
				Bucket: aws.String(destBucket),
				Prefix: aws.String(key),
			})
			g.Expect(listErr).NotTo(HaveOccurred())
			for i := range out.DeleteMarkers {
				if aws.ToString(out.DeleteMarkers[i].Key) == key {
					return true
				}
			}
			return false
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(BeTrue(), "delete marker should be replicated to destination")

		// Per CLDSRV-899, replicated delete markers arrive on cloudserver
		// as a putMetadata but are logged as REST.DELETE.OBJECT with a
		// synthesized DELETE requestURI and the same blanking as the PUT
		// case.
		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaDeleteMarkerEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.DELETE.OBJECT entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/replication-role"),
			"replica delete-marker entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica delete-marker entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica delete-marker entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica delete-marker entry should have blanked referer")
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf("DELETE /%s/%s HTTP/1.1", destBucket, key)),
			"replica delete-marker entry's requestURI should be synthesized to a DELETE on the destination")
	})

	It("logs put-tagging replication on destination as REST.PUT.OBJECT_TAGGING", func(ctx context.Context) {
		key := "crr-tagging.txt"
		content := []byte("data with tags to replicate")

		putResp, err := testaccountClnt.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT to source")
		sourceVersionID := aws.ToString(putResp.VersionId)
		Expect(sourceVersionID).NotTo(BeEmpty(), "source PUT should return a VersionId")

		// Wait for the initial body replication before issuing the tagging
		// change; otherwise the two replication events race.
		Eventually(func(g Gomega) types.ReplicationStatus {
			out, headErr := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    aws.String(key),
			})
			g.Expect(headErr).NotTo(HaveOccurred())
			return out.ReplicationStatus
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(Equal(types.ReplicationStatusCompleted),
				"source initial replication should reach COMPLETED before tagging")

		_, err = testaccountClnt.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{{Key: aws.String("env"), Value: aws.String("e2e")}},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PutObjectTagging on source")

		// PutObjectTagging flips the source version's ReplicationStatus
		// back to PENDING; wait for backbeat to replicate the metadata
		// and the source to settle at COMPLETED again.
		Eventually(func(g Gomega) types.ReplicationStatus {
			out, headErr := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    aws.String(key),
			})
			g.Expect(headErr).NotTo(HaveOccurred())
			return out.ReplicationStatus
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(Equal(types.ReplicationStatusCompleted),
				"source tagging replication should reach COMPLETED")

		// Per CLDSRV-899, replicated PutObjectTagging arrives on
		// cloudserver as a putMetadata (mdOnly=true) but is logged as
		// REST.PUT.OBJECT_TAGGING with requestURI synthesized to
		// PUT /<dst>/<key>?tagging&versionId=<destVid> HTTP/1.1 and
		// the same HTTP-layer blanking as the regular replication PUT.
		// CRR preserves version IDs, so the destination version-id
		// equals the source PUT's VersionId.
		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaTaggingEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.PUT.OBJECT_TAGGING entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/replication-role"),
			"replica tagging entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica tagging entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica tagging entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica tagging entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf(
			"PUT /%s/%s?tagging&versionId=%s HTTP/1.1", destBucket, key, sourceVersionID)),
			"replica tagging entry's requestURI should include ?tagging&versionId")
	})

	It("logs MPU replication on destination as N REST.PUT.OBJECT entries", func(ctx context.Context) {
		key := "crr-mpu.txt"
		const partSize = 5 * 1024 * 1024
		const numParts = 3
		parts := make([][]byte, numParts)
		for i := 0; i < numParts; i++ {
			parts[i] = bytes.Repeat([]byte{byte('a' + i)}, partSize)
		}

		createResp, err := testaccountClnt.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
		})
		Expect(err).NotTo(HaveOccurred(), "CreateMultipartUpload on source")
		uploadID := createResp.UploadId

		completed := make([]types.CompletedPart, numParts)
		for i := 0; i < numParts; i++ {
			partResp, partErr := testaccountClnt.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(sourceBucket),
				Key:        aws.String(key),
				PartNumber: aws.Int32(int32(i + 1)),
				UploadId:   uploadID,
				Body:       bytes.NewReader(parts[i]),
			})
			Expect(partErr).NotTo(HaveOccurred(), "UploadPart %d on source", i+1)
			completed[i] = types.CompletedPart{
				ETag:       partResp.ETag,
				PartNumber: aws.Int32(int32(i + 1)),
			}
		}

		_, err = testaccountClnt.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(sourceBucket),
			Key:             aws.String(key),
			UploadId:        uploadID,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
		})
		Expect(err).NotTo(HaveOccurred(), "CompleteMultipartUpload on source")

		Eventually(func(g Gomega) types.ReplicationStatus {
			out, headErr := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(sourceBucket),
				Key:    aws.String(key),
			})
			g.Expect(headErr).NotTo(HaveOccurred())
			return out.ReplicationStatus
		}).WithTimeout(replicationTimeout).
			WithPolling(replicationPoll).
			Should(Equal(types.ReplicationStatusCompleted),
				"source MPU replication status should reach COMPLETED")

		destOut, err := testaccountClnt.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(destBucket),
			Key:    aws.String(key),
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD on replica")
		Expect(destOut.ReplicationStatus).To(Equal(types.ReplicationStatusReplica),
			"destination MPU object replication status should be REPLICA")

		// Per CLDSRV-899, MPU replication writes one REST.PUT.OBJECT per
		// part on the destination. AWS would log POST.UPLOADS + N PUT.PART
		// + POST.UPLOAD, but cloudserver has no MPU lifecycle on the
		// destination — parts land via N PUT /_/backbeat/data calls + a
		// single stitching putMetadata, so AWS's initiate/parts/complete
		// events correspond to operations that don't happen here.
		var entries []*ParsedLogRecord
		Eventually(func() int {
			entries = findReplicaPutEntries(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return len(entries)
		}, logWaitTimeout, logPollInterval).Should(Equal(numParts),
			"expected %d REST.PUT.OBJECT entries (one per replicated part) under %s", numParts, replicaLogPrefix)

		for i, e := range entries {
			Expect(e.Requester).To(ContainSubstring("assumed-role/replication-role"),
				"part-%d entry's requester should be the replication role", i+1)
			Expect(e.RemoteIP).To(Equal("-"), "part-%d entry should have blanked clientIP", i+1)
			Expect(e.UserAgent).To(Equal("-"), "part-%d entry should have blanked userAgent", i+1)
			Expect(e.Referer).To(Equal("-"), "part-%d entry should have blanked referer", i+1)
			Expect(e.HTTPStatus).To(Equal(200), "part-%d entry should have HTTP 200", i+1)
			Expect(e.RequestURI).To(Equal(fmt.Sprintf("PUT /%s/%s HTTP/1.1", destBucket, key)),
				"part-%d entry's requestURI should be synthesized to a normal S3 PUT", i+1)
		}
	})
})

func setupReplicationBuckets(ctx context.Context, client *s3.Client, src, dst, logTarget string) {
	GinkgoHelper()
	for _, b := range []string{src, dst, logTarget} {
		Expect(createBucketWithRetry(client, b)).To(Succeed())
	}
	for _, b := range []string{src, dst} {
		_, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(b),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		Expect(err).NotTo(HaveOccurred(), "enable versioning on %s", b)
	}

	policy := fmt.Sprintf(`{
		"Version": "2012-10-17",
		"Statement": [{
			"Sid": "AllowLogCourierPutObject",
			"Effect": "Allow",
			"Principal": {"AWS": "%s"},
			"Action": "s3:PutObject",
			"Resource": "arn:aws:s3:::%s/*"
		}]
	}`, logCourierAccountRootARN, logTarget)
	_, err := client.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(logTarget),
		Policy: aws.String(policy),
	})
	Expect(err).NotTo(HaveOccurred(), "put bucket policy on %s", logTarget)

	_, err = client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
		Bucket: aws.String(dst),
		BucketLoggingStatus: &types.BucketLoggingStatus{
			LoggingEnabled: &types.LoggingEnabled{
				TargetBucket: aws.String(logTarget),
				TargetPrefix: aws.String(replicaLogPrefix),
			},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "configure bucket logging on %s", dst)

	_, err = client.PutBucketReplication(ctx, &s3.PutBucketReplicationInput{
		Bucket: aws.String(src),
		ReplicationConfiguration: &types.ReplicationConfiguration{
			Role: aws.String(fmt.Sprintf("%s,%s", replicationRoleARN, replicationRoleARN)),
			Rules: []types.ReplicationRule{
				{
					ID:     aws.String("e2e-crr"),
					Status: types.ReplicationRuleStatusEnabled,
					Filter: &types.ReplicationRuleFilter{Prefix: aws.String("")},
					Destination: &types.Destination{
						Bucket:       aws.String("arn:aws:s3:::" + dst),
						StorageClass: types.StorageClass("sf"),
					},
				},
			},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "configure bucket replication on %s", src)
}

func cleanupReplicationBuckets(ctx context.Context, client *s3.Client, src, dst, logTarget string) {
	for _, b := range []string{src, dst, logTarget} {
		emptyVersionedBucket(ctx, client, b)
		_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(b)})
	}
}

// findReplicaPutEntry returns the REST.PUT.OBJECT log entry for key
// on bucket in logBucket under prefix, or nil if no such entry has
// been delivered yet. Caller wraps in Eventually.
func findReplicaPutEntry(client *s3.Client, logBucket, prefix, bucket, key string, since time.Time) *ParsedLogRecord {
	logs, err := fetchLogsFromPrefix(client, logBucket, prefix, since)
	if err != nil {
		return nil
	}
	for _, r := range logs {
		if r.Operation == "REST.PUT.OBJECT" && r.Bucket == bucket && r.Key == key {
			return r
		}
	}
	return nil
}

// findReplicaDeleteMarkerEntry returns the REST.DELETE.OBJECT log entry
// for key on bucket in logBucket under prefix, or nil if no such entry
// has been delivered yet. Caller wraps in Eventually.
func findReplicaDeleteMarkerEntry(client *s3.Client, logBucket, prefix, bucket, key string, since time.Time) *ParsedLogRecord {
	logs, err := fetchLogsFromPrefix(client, logBucket, prefix, since)
	if err != nil {
		return nil
	}
	for _, r := range logs {
		if r.Operation == "REST.DELETE.OBJECT" && r.Bucket == bucket && r.Key == key {
			return r
		}
	}
	return nil
}

// findReplicaTaggingEntry returns the REST.PUT.OBJECT_TAGGING log entry
// for key on bucket in logBucket under prefix, or nil if no such entry
// has been delivered yet. Caller wraps in Eventually.
func findReplicaTaggingEntry(client *s3.Client, logBucket, prefix, bucket, key string, since time.Time) *ParsedLogRecord {
	logs, err := fetchLogsFromPrefix(client, logBucket, prefix, since)
	if err != nil {
		return nil
	}
	for _, r := range logs {
		if r.Operation == "REST.PUT.OBJECT_TAGGING" && r.Bucket == bucket && r.Key == key {
			return r
		}
	}
	return nil
}

// findReplicaPutEntries returns all REST.PUT.OBJECT log entries for key
// on bucket in logBucket under prefix that have been delivered. Caller
// wraps in Eventually to wait for the expected count.
func findReplicaPutEntries(client *s3.Client, logBucket, prefix, bucket, key string, since time.Time) []*ParsedLogRecord {
	logs, err := fetchLogsFromPrefix(client, logBucket, prefix, since)
	if err != nil {
		return nil
	}
	var matches []*ParsedLogRecord
	for _, r := range logs {
		if r.Operation == "REST.PUT.OBJECT" && r.Bucket == bucket && r.Key == key {
			matches = append(matches, r)
		}
	}
	return matches
}

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
