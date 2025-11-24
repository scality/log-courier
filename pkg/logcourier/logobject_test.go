package logcourier_test

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
)

var _ = Describe("LogObjectBuilder", func() {
	var builder *logcourier.LogObjectBuilder

	BeforeEach(func() {
		builder = logcourier.NewLogObjectBuilder()
	})

	Describe("Build", func() {
		It("should build log object from records", func() {
			now := time.Now()
			records := []logcourier.LogRecord{
				{
					BucketOwner:         "owner1",
					BucketName:          "test-bucket",
					StartTime:           now,
					ClientIP:            "192.168.1.1",
					Requester:           "arn:aws:iam::123456789012:user/alice",
					ReqID:               "ABC123XYZ",
					Operation:           "REST.GET.OBJECT",
					ObjectKey:           "my-file.txt",
					RequestURI:          "GET /test-bucket/my-file.txt HTTP/1.1",
					HttpCode:            200,
					ErrorCode:           "",
					BytesSent:           1024,
					ObjectSize:          1024,
					TotalTime:           45.5,
					TurnAroundTime:      15.2,
					Referer:             "http://example.com",
					UserAgent:           "curl/7.64.1",
					VersionID:           "",
					SignatureVersion:    "SigV4",
					CipherSuite:         "ECDHE-RSA-AES128-GCM-SHA256",
					AuthenticationType:  "AuthHeader",
					HostHeader:          "s3.amazonaws.com",
					TlsVersion:          "TLSv1.2",
					AclRequired:         "Yes",
					LoggingTargetPrefix: "logs/",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj.Key).NotTo(BeEmpty())
			Expect(obj.Content).NotTo(BeEmpty())
		})

		It("should fail with empty records", func() {
			_, err := builder.Build([]logcourier.LogRecord{})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("no records"))
		})

		It("should generate key in AWS format with prefix", func() {
			now := time.Date(2025, 10, 28, 14, 30, 45, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					LoggingTargetPrefix: "mylogs/",
					StartTime:           now,
					Timestamp:           now,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			Expect(obj.Key).To(HavePrefix("mylogs/2025-10-28-14-30-45-"))

			// Verify unique string is 16 hex characters
			parts := strings.Split(obj.Key, "-")
			Expect(parts).To(HaveLen(7)) // mylogs/2025, 10, 28, 14, 30, 45, UNIQUE
			uniqueString := parts[6]
			Expect(uniqueString).To(HaveLen(16))
			Expect(uniqueString).To(MatchRegexp("^[0-9A-F]+$"))
		})

		It("should generate key without prefix when prefix is empty", func() {
			now := time.Date(2025, 10, 28, 14, 30, 45, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					LoggingTargetPrefix: "",
					StartTime:           now,
					Timestamp:           now,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			// Key format without prefix: YYYY-mm-DD-HH-MM-SS-UniqueString
			Expect(obj.Key).To(MatchRegexp(`^2025-10-28-14-30-45-[0-9A-F]{16}$`))
		})

		It("should format timestamp in AWS format", func() {
			// AWS format: [DD/MMM/YYYY:HH:MM:SS +0000]
			startTime := time.Date(2025, 10, 28, 14, 30, 45, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					LoggingTargetPrefix: "",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			// Should contain: [28/Oct/2025:14:30:45 +0000]
			Expect(content).To(ContainSubstring("[28/Oct/2025:14:30:45 +0000]"))
		})

		It("should use dash for empty string fields", func() {
			startTime := time.Date(2025, 10, 28, 10, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketOwner:         "",
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ClientIP:            "",
					Requester:           "",
					ReqID:               "ABC123",
					Operation:           "GET",
					ObjectKey:           "",
					RequestURI:          "",
					ErrorCode:           "",
					VersionID:           "",
					SignatureVersion:    "",
					LoggingTargetPrefix: "",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Empty fields should be "-"
			expectedLine := "- test-bucket [28/Oct/2025:10:00:00 +0000] - - ABC123 GET - - 0 - 0 0 0 0 - - - - - - - - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should quote Request-URI, Referer, and User-Agent fields", func() {
			startTime := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					RequestURI:          "GET /bucket/key?versionId=123 HTTP/1.1",
					Referer:             "http://example.com/page",
					UserAgent:           "Mozilla/5.0",
					LoggingTargetPrefix: "",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Exact match - HttpURL, Referer, and UserAgent should be quoted
			expectedLine := `- test-bucket [28/Oct/2025:12:00:00 +0000] - - - - - "GET /bucket/key?versionId=123 HTTP/1.1" 0 - 0 0 0 0 "http://example.com/page" "Mozilla/5.0" - - - - - - -`
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should format numeric fields correctly", func() {
			startTime := time.Date(2025, 10, 28, 13, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					HttpCode:            200,
					BytesSent:           1024,
					ObjectSize:          2048,
					TotalTime:           45.5,
					TurnAroundTime:      15.2,
					LoggingTargetPrefix: "",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Exact match - numeric fields should be formatted correctly
			expectedLine := "- test-bucket [28/Oct/2025:13:00:00 +0000] - - - - - - 200 - 1024 2048 45.5 15.2 - - - - - - - - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should handle zero values correctly", func() {
			startTime := time.Date(2025, 10, 28, 14, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ReqID:               "test-123",
					HttpCode:            200,
					BytesSent:           0,
					ObjectSize:          0,
					TotalTime:           0,
					TurnAroundTime:      0,
					LoggingTargetPrefix: "",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Exact match - zero values should be output as "0" not "-"
			expectedLine := "- test-bucket [28/Oct/2025:14:00:00 +0000] - - test-123 - - - 200 - 0 0 0 0 - - - - - - - - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should handle multiple records", func() {
			startTime := time.Date(2025, 10, 28, 15, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ReqID:               "req-1",
					Operation:           "GET",
					LoggingTargetPrefix: "",
				},
				{
					BucketName:          "test-bucket",
					StartTime:           startTime.Add(1 * time.Second),
					ReqID:               "req-2",
					Operation:           "PUT",
					LoggingTargetPrefix: "",
				},
				{
					BucketName:          "test-bucket",
					StartTime:           startTime.Add(2 * time.Second),
					ReqID:               "req-3",
					Operation:           "DELETE",
					LoggingTargetPrefix: "",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Should have 3 lines (one per record)
			Expect(lines).To(HaveLen(3))

			expectedLine1 := "- test-bucket [28/Oct/2025:15:00:00 +0000] - - req-1 GET - - 0 - 0 0 0 0 - - - - - - - - -"
			expectedLine2 := "- test-bucket [28/Oct/2025:15:00:01 +0000] - - req-2 PUT - - 0 - 0 0 0 0 - - - - - - - - -"
			expectedLine3 := "- test-bucket [28/Oct/2025:15:00:02 +0000] - - req-3 DELETE - - 0 - 0 0 0 0 - - - - - - - - -"

			Expect(lines[0]).To(Equal(expectedLine1))
			Expect(lines[1]).To(Equal(expectedLine2))
			Expect(lines[2]).To(Equal(expectedLine3))
		})

		It("should produce valid AWS S3 Server Access Log format", func() {
			// Test with a complete record to ensure the format matches AWS specification
			startTime := time.Date(2025, 10, 28, 14, 30, 45, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketOwner:         "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be",
					BucketName:          "my-bucket",
					StartTime:           startTime,
					Timestamp:           startTime,
					ClientIP:            "192.0.2.3",
					Requester:           "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be",
					ReqID:               "3E57427F3EXAMPLE",
					Operation:           "REST.GET.VERSIONING",
					ObjectKey:           "",
					RequestURI:          "GET /my-bucket?versioning HTTP/1.1",
					HttpCode:            200,
					ErrorCode:           "",
					BytesSent:           113,
					ObjectSize:          0,
					TotalTime:           7,
					TurnAroundTime:      4,
					Referer:             "",
					UserAgent:           "S3Console/0.4",
					VersionID:           "",
					SignatureVersion:    "SigV4",
					CipherSuite:         "ECDHE-RSA-AES128-GCM-SHA256",
					AuthenticationType:  "AuthHeader",
					HostHeader:          "my-bucket.s3.amazonaws.com",
					TlsVersion:          "TLSv1.2",
					AclRequired:         "-",
					LoggingTargetPrefix: "logs/",
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")
			Expect(lines).To(HaveLen(1))

			expectedLine := "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be my-bucket [28/Oct/2025:14:30:45 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - \"GET /my-bucket?versioning HTTP/1.1\" 200 - 113 0 7 4 - \"S3Console/0.4\" - SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader my-bucket.s3.amazonaws.com TLSv1.2 -"
			Expect(lines[0]).To(Equal(expectedLine))
		})
	})
})
