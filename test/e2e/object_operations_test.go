package e2e_test

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object Operations", func() {
	var ctx *E2ETestContext

	BeforeEach(func() {
		ctx = setupE2ETest()
	})

	AfterEach(func() {
		cleanupE2ETest(ctx)
	})

	It("logs basic CRUD operations (PUT/GET/HEAD/DELETE)", func() {
		testKey := "test-object.txt"
		testContent := []byte("test data for basic CRUD operations")

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET operation should succeed")

		_, err = ctx.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD operation should succeed")

		_, err = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE operation should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.GET.OBJECT", testKey, 200).WithBytesSent(int64(len(testContent))),
			ctx.ObjectOp("REST.HEAD.OBJECT", testKey, 200),
			ctx.ObjectOp("REST.DELETE.OBJECT", testKey, 204),
		)
	})

	It("logs PUT operations with ACL and metadata", func() {
		keyWithACL := "object-with-acl.txt"
		keyWithMeta := "object-with-metadata.txt"
		testContent := []byte("test data")

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(keyWithACL),
			Body:   bytes.NewReader(testContent),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT with ACL should succeed")

		_, err = ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(keyWithMeta),
			Body:   bytes.NewReader(testContent),
			Metadata: map[string]string{
				"custom-key": "custom-value",
				"author":     "test-suite",
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT with metadata should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", keyWithACL, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.PUT.OBJECT", keyWithMeta, 200).WithObjectSize(int64(len(testContent))),
		)
	})

	It("logs list and copy operations", func() {
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("list-test/object-%d.txt", i)
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("content-%d", i))),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT object %d should succeed", i)
		}

		_, err := ctx.S3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(ctx.SourceBucket),
		})
		Expect(err).NotTo(HaveOccurred(), "LIST objects should succeed")

		_, err = ctx.S3Client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket: aws.String(ctx.SourceBucket),
			Prefix: aws.String("list-test/"),
		})
		Expect(err).NotTo(HaveOccurred(), "LIST with prefix should succeed")

		sourceKey := "list-test/object-1.txt"
		destKey := "list-test/object-1-copy.txt"
		_, err = ctx.S3Client.CopyObject(context.Background(), &s3.CopyObjectInput{
			Bucket:     aws.String(ctx.SourceBucket),
			Key:        aws.String(destKey),
			CopySource: aws.String(fmt.Sprintf("%s/%s", ctx.SourceBucket, sourceKey)),
		})
		Expect(err).NotTo(HaveOccurred(), "COPY object should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", "list-test/object-1.txt", 200),
			ctx.ObjectOp("REST.PUT.OBJECT", "list-test/object-2.txt", 200),
			ctx.ObjectOp("REST.PUT.OBJECT", "list-test/object-3.txt", 200),
			ctx.ObjectOp("REST.PUT.OBJECT", "list-test/object-4.txt", 200),
			ctx.ObjectOp("REST.PUT.OBJECT", "list-test/object-5.txt", 200),
			ctx.BucketOp("REST.GET.BUCKET", 200),
			ctx.BucketOp("REST.GET.BUCKET", 200),
			ctx.ObjectOp("REST.COPY.OBJECT_GET", sourceKey, 200).WithObjectSize(int64(len("content-1"))),
			ctx.ObjectOp("REST.COPY.OBJECT", destKey, 200).WithObjectSize(int64(len("content-1"))),
		)
	})

	It("logs object ACL operations", func() {
		testKey := "acl-test-object.txt"
		testContent := []byte("test data for ACL operations")

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		_, err = ctx.S3Client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET ACL should succeed")

		_, err = ctx.S3Client.PutObjectAcl(context.Background(), &s3.PutObjectAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			ACL:    types.ObjectCannedACLPublicRead,
		})
		Expect(err).NotTo(HaveOccurred(), "PUT ACL should succeed")

		_, err = ctx.S3Client.GetObjectAcl(context.Background(), &s3.GetObjectAclInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET ACL verification should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.GET.ACL", testKey, 200),
			ctx.ObjectOp("REST.PUT.ACL", testKey, 200),
			ctx.ObjectOp("REST.GET.ACL", testKey, 200),
		)
	})

	It("logs object tagging operations", func() {
		testKey := "tagging-test-object.txt"
		testContent := []byte("test data for tagging operations")

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		_, err = ctx.S3Client.PutObjectTagging(context.Background(), &s3.PutObjectTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Tagging: &types.Tagging{
				TagSet: []types.Tag{
					{Key: aws.String("Environment"), Value: aws.String("test")},
					{Key: aws.String("Owner"), Value: aws.String("e2e-suite")},
				},
			},
		})
		Expect(err).NotTo(HaveOccurred(), "PUT tagging should succeed")

		_, err = ctx.S3Client.GetObjectTagging(context.Background(), &s3.GetObjectTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET tagging should succeed")

		_, err = ctx.S3Client.DeleteObjectTagging(context.Background(), &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE tagging should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.PUT.TAGGING", testKey, 200),
			ctx.ObjectOp("REST.GET.TAGGING", testKey, 200),
			ctx.ObjectOp("REST.DELETE.TAGGING", testKey, 204),
		)
	})

	It("logs range GET operations", func() {
		testKey := "range-test-object.txt"
		testContent := []byte("0123456789ABCDEFGHIJ") // 20 bytes

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT operation should succeed")

		// Range GET - first 10 bytes
		rangeHeader := "bytes=0-9"
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Range:  aws.String(rangeHeader),
		})
		Expect(err).NotTo(HaveOccurred(), "Range GET should succeed")

		// Range GET - last 5 bytes
		rangeHeader2 := "bytes=-5"
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Range:  aws.String(rangeHeader2),
		})
		Expect(err).NotTo(HaveOccurred(), "Range GET (suffix) should succeed")

		// Range GET - middle portion (bytes 5-14)
		rangeHeader3 := "bytes=5-14"
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Range:  aws.String(rangeHeader3),
		})
		Expect(err).NotTo(HaveOccurred(), "Range GET (middle) should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.GET.OBJECT", testKey, 206).WithBytesSent(10),
			ctx.ObjectOp("REST.GET.OBJECT", testKey, 206).WithBytesSent(5),
			ctx.ObjectOp("REST.GET.OBJECT", testKey, 206).WithBytesSent(10),
		)
	})

	It("logs empty object operations (0 bytes)", func() {
		testKey := "empty-object.txt"
		emptyContent := []byte{}

		_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(emptyContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT empty object should succeed")

		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "GET empty object should succeed")

		_, err = ctx.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD empty object should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(0),
			ctx.ObjectOp("REST.GET.OBJECT", testKey, 200).WithBytesSent(0),
			ctx.ObjectOp("REST.HEAD.OBJECT", testKey, 200).WithObjectSize(0),
		)
	})

	It("logs batch delete operations", func() {
		objectKeys := []string{
			"delete-test/object-1.txt",
			"delete-test/object-2.txt",
			"delete-test/object-3.txt",
		}

		for i, key := range objectKeys {
			_, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(ctx.SourceBucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader([]byte(fmt.Sprintf("content-%d", i+1))),
			})
			Expect(err).NotTo(HaveOccurred(), "PUT object %s should succeed", key)
		}

		var objectIds []types.ObjectIdentifier
		for _, key := range objectKeys {
			objectIds = append(objectIds, types.ObjectIdentifier{
				Key: aws.String(key),
			})
		}

		_, err := ctx.S3Client.DeleteObjects(context.Background(), &s3.DeleteObjectsInput{
			Bucket: aws.String(ctx.SourceBucket),
			Delete: &types.Delete{
				Objects: objectIds,
				Quiet:   aws.Bool(false),
			},
		})
		Expect(err).NotTo(HaveOccurred(), "Multi-object delete should succeed")

		ctx.VerifyLogs(
			ctx.ObjectOp("REST.PUT.OBJECT", objectKeys[0], 200),
			ctx.ObjectOp("REST.PUT.OBJECT", objectKeys[1], 200),
			ctx.ObjectOp("REST.PUT.OBJECT", objectKeys[2], 200),
			ctx.BucketOp("BATCH.DELETE.OBJECT", 204),
			ctx.BucketOp("BATCH.DELETE.OBJECT", 204),
			ctx.BucketOp("BATCH.DELETE.OBJECT", 204),
			ctx.BucketOp("REST.POST.MULTI_OBJECT_DELETE", 200),
		)
	})

	It("logs object operations with versioning enabled", func() {
		testKey := "versioned-object.txt"
		testContent := []byte("version content")

		// Enable versioning
		_, err := ctx.S3Client.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
			Bucket: aws.String(ctx.SourceBucket),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		})
		Expect(err).NotTo(HaveOccurred(), "Enable versioning should succeed")

		// PUT object (creates first version)
		putResp, err := ctx.S3Client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
			Body:   bytes.NewReader(testContent),
		})
		Expect(err).NotTo(HaveOccurred(), "PUT object version should succeed")
		versionID := putResp.VersionId

		// DELETE object without versionId (creates delete marker)
		deleteResp, err := ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: aws.String(ctx.SourceBucket),
			Key:    aws.String(testKey),
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE object should succeed")
		deleteMarkerVersionID := deleteResp.VersionId

		// GET object WITH ?versionId= (should log the versionId)
		_, err = ctx.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: versionID,
		})
		Expect(err).NotTo(HaveOccurred(), "GET object version should succeed")

		// HEAD object WITH ?versionId= (should log the versionId)
		_, err = ctx.S3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: versionID,
		})
		Expect(err).NotTo(HaveOccurred(), "HEAD object version should succeed")

		// DELETE object WITH ?versionId= (should log the versionId)
		_, err = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: deleteMarkerVersionID,
		})
		Expect(err).NotTo(HaveOccurred(), "DELETE delete marker should succeed")

		logs := ctx.VerifyLogs(
			ctx.BucketOp("REST.PUT.VERSIONING", 200),
			ctx.ObjectOp("REST.PUT.OBJECT", testKey, 200).WithObjectSize(int64(len(testContent))),
			ctx.ObjectOp("REST.DELETE.OBJECT", testKey, 204),
			ctx.ObjectOp("REST.GET.OBJECT", testKey, 200).WithBytesSent(int64(len(testContent))),
			ctx.ObjectOp("REST.HEAD.OBJECT", testKey, 200),
			ctx.ObjectOp("REST.DELETE.OBJECT", testKey, 204),
		)

		// VersionID only appears in logs when client uses ?versionId= parameter
		By("verifying VersionID is '-' for operations without ?versionId= parameter")
		Expect(logs[1].VersionID).To(Equal("-"),
			"PUT without ?versionId= should have '-' as VersionID")
		Expect(logs[2].VersionID).To(Equal("-"),
			"DELETE without ?versionId= should have '-' as VersionID")

		By("verifying VersionID matches for operations with ?versionId= parameter")
		Expect(logs[3].VersionID).To(Equal(*versionID),
			"GET with ?versionId= should log the requested version")
		Expect(logs[4].VersionID).To(Equal(*versionID),
			"HEAD with ?versionId= should log the requested version")
		Expect(logs[5].VersionID).To(Equal(*deleteMarkerVersionID),
			"DELETE with ?versionId= should log the requested version")

		// Cleanup: Delete remaining version
		_, _ = ctx.S3Client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket:    aws.String(ctx.SourceBucket),
			Key:       aws.String(testKey),
			VersionId: versionID,
		})
	})
})
