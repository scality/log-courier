package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// Workbench defaults. In workbench's setup-vault, vault accountSeeds
	// pre-provision an account 123456789012 / testaccount that owns a
	// scality-internal/replication-role. Integration provisions its own
	// role at test setup time and supplies the ARN via E2E_REPLICATION_ROLE_ARN.
	defaultReplicationRoleARN             = "arn:aws:iam::123456789012:role/scality-internal/replication-role"
	defaultReplicationDestinationLocation = "sf"

	replicationTimeout = 30 * time.Second
	replicationPoll    = 2 * time.Second

	replicaLogPrefix = "dst/"
	sourceLogPrefix  = "src/"
)

var (
	// replicationRoleARN identifies the role Backbeat assumes to perform
	// replication writes. Harnesses pre-provision it.
	replicationRoleARN = envOrDefault("E2E_REPLICATION_ROLE_ARN", defaultReplicationRoleARN) //nolint:gochecknoglobals // Env-driven test fixture initialized at package load

	// replicationRoleName is the trailing component of replicationRoleARN
	// and is also what appears in the access-log Requester field as
	// "assumed-role/<roleName>/<sessionName>".
	replicationRoleName = path.Base(replicationRoleARN) //nolint:gochecknoglobals // Derived from replicationRoleARN at package load

	// replicationDestinationLocation is the cloudserver replication endpoint
	// site name; must match what is configured under env_replication_endpoints
	// in the deployed environment.
	replicationDestinationLocation = envOrDefault("E2E_REPLICATION_DESTINATION_LOCATION", defaultReplicationDestinationLocation) //nolint:gochecknoglobals // Env-driven test fixture initialized at package load

	// Root principal of the log-courier account, granted PutObject on
	// log target buckets so log-courier can deliver across accounts.
	// See "Test account model" in suite_test.go.
	logCourierAccountRootARN = "arn:aws:iam::" + logCourierAccountID + ":root" //nolint:gochecknoglobals // Derived from env-driven logCourierAccountID at package load
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

		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaPutEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.PUT.OBJECT entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
			"replica entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf("PUT /%s/%s HTTP/1.1", destBucket, key)),
			"replica entry's requestURI should be synthesized to a normal S3 PUT")

		var sourceGet *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			sourceGet = findSourceGetEntry(testaccountClnt, logBucket, sourceLogPrefix, sourceBucket, key, testStartTime)
			return sourceGet
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"source REST.GET.OBJECT entry should be delivered under %s", sourceLogPrefix)

		Expect(sourceGet.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
			"source GET entry's requester should be the replication role")
		Expect(sourceGet.HTTPStatus).To(Equal(200))
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
		// creates a delete marker.
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

		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaDeleteMarkerEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.DELETE.OBJECT entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
			"replica delete-marker entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica delete-marker entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica delete-marker entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica delete-marker entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
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

		// Wait for the initial body replication before issuing the tagging change.
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
		// back to PENDING; wait for the source to settle at COMPLETED again.
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

		// A replicated PutObjectTagging is logged as
		// REST.PUT.OBJECT_TAGGING with requestURI synthesized to
		// PUT /<dst>/<key>?tagging&versionId=<destVid> HTTP/1.1.
		// CRR preserves version IDs, so the destination version-id
		// equals the source PUT's VersionId.
		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaTaggingEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.PUT.OBJECT_TAGGING entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
			"replica tagging entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica tagging entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica tagging entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica tagging entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf(
			"PUT /%s/%s?tagging&versionId=%s HTTP/1.1", destBucket, key, sourceVersionID)),
			"replica tagging entry's requestURI should include ?tagging&versionId")
	})

	It("logs delete-tagging replication on destination as REST.PUT.OBJECT_TAGGING", func(ctx context.Context) {
		key := "crr-delete-tagging.txt"
		content := []byte("data with tags to be cleared")

		putResp, err := testaccountClnt.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(sourceBucket),
			Key:     aws.String(key),
			Body:    bytes.NewReader(content),
			Tagging: aws.String("env=e2e"),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT to source")
		sourceVersionID := aws.ToString(putResp.VersionId)
		Expect(sourceVersionID).NotTo(BeEmpty(), "source PUT should return a VersionId")

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
				"source initial replication should reach COMPLETED before tag delete")

		_, err = testaccountClnt.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
		})
		Expect(err).NotTo(HaveOccurred(), "DeleteObjectTagging on source")

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
				"source delete-tagging replication should reach COMPLETED")

		// A replicated DeleteObjectTagging is logged as REST.PUT.OBJECT_TAGGING
		// with an empty tag set with the same URI as a PutObjectTagging.
		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaTaggingEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.PUT.OBJECT_TAGGING entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
			"replica delete-tagging entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica delete-tagging entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica delete-tagging entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica delete-tagging entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf(
			"PUT /%s/%s?tagging&versionId=%s HTTP/1.1", destBucket, key, sourceVersionID)),
			"replica delete-tagging entry's requestURI should include ?tagging&versionId")
	})

	It("logs put-acl replication on destination as REST.PUT.ACL", func(ctx context.Context) {
		key := "crr-acl.txt"
		content := []byte("data with ACL to replicate")

		putResp, err := testaccountClnt.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(content),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT to source")
		sourceVersionID := aws.ToString(putResp.VersionId)
		Expect(sourceVersionID).NotTo(BeEmpty(), "source PUT should return a VersionId")

		// Wait for the initial body replication before issuing the ACL change.
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
				"source initial replication should reach COMPLETED before ACL change")

		// Use a canned ACL that actually changes the ACL set.
		_, err = testaccountClnt.PutObjectAcl(ctx, &s3.PutObjectAclInput{
			Bucket: aws.String(sourceBucket),
			Key:    aws.String(key),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		Expect(err).NotTo(HaveOccurred(), "PutObjectAcl on source")

		// PutObjectAcl flips the source version's ReplicationStatus
		// back to PENDING; wait for the source to settle at COMPLETED again.
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
				"source ACL replication should reach COMPLETED")

		// A replicated PutObjectAcl is logged as REST.PUT.ACL
		// with requestURI synthesized to PUT /<dst>/<key>?acl&versionId=<destVid> HTTP/1.1,
		// and aclRequired = "Yes". CRR preserves version IDs, so the
		// destination version-id equals the source PUT's VersionId.
		var replica *ParsedLogRecord
		Eventually(func() *ParsedLogRecord {
			replica = findReplicaACLEntry(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return replica
		}, logWaitTimeout, logPollInterval).ShouldNot(BeNil(),
			"replica REST.PUT.ACL entry should be delivered under %s", replicaLogPrefix)

		Expect(replica.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
			"replica ACL entry's requester should be the replication role")
		Expect(replica.RemoteIP).To(Equal("-"), "replica ACL entry should have blanked clientIP")
		Expect(replica.UserAgent).To(Equal("-"), "replica ACL entry should have blanked userAgent")
		Expect(replica.Referer).To(Equal("-"), "replica ACL entry should have blanked referer")
		Expect(replica.HTTPStatus).To(Equal(200))
		Expect(replica.RequestURI).To(Equal(fmt.Sprintf(
			"PUT /%s/%s?acl&versionId=%s HTTP/1.1", destBucket, key, sourceVersionID)),
			"replica ACL entry's requestURI should include ?acl&versionId")
		Expect(replica.ACLRequired).To(Equal("Yes"),
			"replica ACL entry should have aclRequired = Yes")
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

		// MPU replication writes one REST.PUT.OBJECT per part on the destination,
		// without any POST.UPLOADS / POST.UPLOAD.
		var entries []*ParsedLogRecord
		Eventually(func() int {
			entries = findReplicaPutEntries(testaccountClnt, logBucket, replicaLogPrefix, destBucket, key, testStartTime)
			return len(entries)
		}, logWaitTimeout, logPollInterval).Should(Equal(numParts),
			"expected %d REST.PUT.OBJECT entries (one per replicated part) under %s", numParts, replicaLogPrefix)

		for i, e := range entries {
			Expect(e.Requester).To(ContainSubstring("assumed-role/"+replicationRoleName),
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

	_, err = client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
		Bucket: aws.String(src),
		BucketLoggingStatus: &types.BucketLoggingStatus{
			LoggingEnabled: &types.LoggingEnabled{
				TargetBucket: aws.String(logTarget),
				TargetPrefix: aws.String(sourceLogPrefix),
			},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "configure bucket logging on %s", src)

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
						StorageClass: types.StorageClass(replicationDestinationLocation),
					},
				},
			},
		},
	})
	Expect(err).NotTo(HaveOccurred(), "configure bucket replication on %s", src)
}

func cleanupReplicationBuckets(ctx context.Context, client *s3.Client, src, dst, logTarget string) {
	drainBucketLogs(ctx, client, dst, logTarget, replicaLogPrefix)
	drainBucketLogs(ctx, client, src, logTarget, sourceLogPrefix)

	for _, b := range []string{src, dst, logTarget} {
		if err := deleteBucketWithRetry(client, b); err != nil {
			fmt.Printf("Warning: failed to delete bucket %s: %v\n", b, err)
		}
	}
}

func drainBucketLogs(ctx context.Context, client *s3.Client, bucket, logTarget, prefix string) {
	GinkgoHelper()
	timeBeforeDisable := time.Now().Add(-2 * time.Second)
	_, err := client.PutBucketLogging(ctx, &s3.PutBucketLoggingInput{
		Bucket:              aws.String(bucket),
		BucketLoggingStatus: &types.BucketLoggingStatus{},
	})
	if err != nil {
		fmt.Printf("Warning: failed to disable logging on %s: %v\n", bucket, err)
		return
	}
	Eventually(func() bool {
		logs, fetchErr := fetchLogsFromPrefix(client, logTarget, prefix, timeBeforeDisable)
		if fetchErr != nil {
			return false
		}
		for _, log := range logs {
			if log.Operation == opPutBucketLogging &&
				log.Bucket == bucket &&
				log.Time.After(timeBeforeDisable) {
				return true
			}
		}
		return false
	}).WithTimeout(30 * time.Second).
		WithPolling(2 * time.Second).
		Should(BeTrue())
}

// findSourceGetEntry returns the REST.GET.OBJECT log entry for key on
// bucket in logBucket under prefix, or nil if no such entry has been
// delivered yet. Caller wraps in Eventually.
func findSourceGetEntry(client *s3.Client, logBucket, prefix, bucket, key string, since time.Time) *ParsedLogRecord {
	logs, err := fetchLogsFromPrefix(client, logBucket, prefix, since)
	if err != nil {
		return nil
	}
	for _, r := range logs {
		if r.Operation == "REST.GET.OBJECT" && r.Bucket == bucket && r.Key == key {
			return r
		}
	}
	return nil
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

// findReplicaACLEntry returns the REST.PUT.ACL log entry for key on
// bucket in logBucket under prefix, or nil if no such entry has been
// delivered yet. Caller wraps in Eventually.
func findReplicaACLEntry(client *s3.Client, logBucket, prefix, bucket, key string, since time.Time) *ParsedLogRecord {
	logs, err := fetchLogsFromPrefix(client, logBucket, prefix, since)
	if err != nil {
		return nil
	}
	for _, r := range logs {
		if r.Operation == "REST.PUT.ACL" && r.Bucket == bucket && r.Key == key {
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
