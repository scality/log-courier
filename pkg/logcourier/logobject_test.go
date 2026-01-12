package logcourier_test

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/testutil"
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
					BucketOwner:         testutil.StrPtr("owner1"),
					BucketName:          "test-bucket",
					StartTime:           now,
					ClientIP:            testutil.StrPtr("192.168.1.1"),
					Requester:           testutil.StrPtr("arn:aws:iam::123456789012:user/alice"),
					ReqID:               "ABC123XYZ",
					Operation:           testutil.StrPtr("REST.GET.OBJECT"),
					ObjectKey:           testutil.StrPtr("my-file.txt"),
					RequestURI:          testutil.StrPtr("GET /test-bucket/my-file.txt HTTP/1.1"),
					HttpCode:            testutil.Uint16Ptr(200),
					ErrorCode:           testutil.StrPtr(""),
					BytesSent:           testutil.Uint64Ptr(1024),
					ObjectSize:          testutil.Uint64Ptr(1024),
					TotalTime:           testutil.Float32Ptr(45.5),
					TurnAroundTime:      testutil.Float32Ptr(15.2),
					Referer:             testutil.StrPtr("http://example.com"),
					UserAgent:           testutil.StrPtr("curl/7.64.1"),
					VersionID:           testutil.StrPtr(""),
					SignatureVersion:    testutil.StrPtr("SigV4"),
					CipherSuite:         testutil.StrPtr("ECDHE-RSA-AES128-GCM-SHA256"),
					AuthenticationType:  testutil.StrPtr("AuthHeader"),
					HostHeader:          testutil.StrPtr("s3.amazonaws.com"),
					TlsVersion:          testutil.StrPtr("TLSv1.2"),
					AclRequired:         testutil.StrPtr("Yes"),
					Timestamp:           now,
					InsertedAt:          time.Now(),
					LoggingTargetPrefix: "logs/",
					RaftSessionID:       0,
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
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
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
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
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
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
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
					BucketOwner:         testutil.StrPtr(""),
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ClientIP:            testutil.StrPtr(""),
					Requester:           testutil.StrPtr(""),
					ReqID:               "ABC123",
					Operation:           testutil.StrPtr("GET"),
					ObjectKey:           testutil.StrPtr(""),
					RequestURI:          testutil.StrPtr(""),
					HttpCode:            testutil.Uint16Ptr(0),
					ErrorCode:           testutil.StrPtr(""),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					VersionID:           testutil.StrPtr(""),
					SignatureVersion:    testutil.StrPtr(""),
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Empty fields should be "-"
			expectedLine := "- test-bucket [28/Oct/2025:10:00:00 +0000] - - ABC123 GET - - 0 - 0 0 0 0 - - - - - - - - - - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should quote Request-URI, Referer, and User-Agent fields", func() {
			startTime := time.Date(2025, 10, 28, 12, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					RequestURI:          testutil.StrPtr("GET /bucket/key?versionId=123 HTTP/1.1"),
					Referer:             testutil.StrPtr("http://example.com/page"),
					UserAgent:           testutil.StrPtr("Mozilla/5.0"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Exact match - HttpURL, Referer, and UserAgent should be quoted
			expectedLine := `- test-bucket [28/Oct/2025:12:00:00 +0000] - - - - - "GET /bucket/key?versionId=123 HTTP/1.1" 0 - 0 0 0 0 "http://example.com/page" "Mozilla/5.0" - - - - - - - - -`
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should format numeric fields correctly", func() {
			startTime := time.Date(2025, 10, 28, 13, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					HttpCode:            testutil.Uint16Ptr(200),
					BytesSent:           testutil.Uint64Ptr(1024),
					ObjectSize:          testutil.Uint64Ptr(2048),
					TotalTime:           testutil.Float32Ptr(45.5),
					TurnAroundTime:      testutil.Float32Ptr(15.2),
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Exact match - numeric fields should be formatted correctly
			expectedLine := "- test-bucket [28/Oct/2025:13:00:00 +0000] - - - - - - 200 - 1024 2048 45.5 15.2 - - - - - - - - - - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should handle zero values correctly", func() {
			startTime := time.Date(2025, 10, 28, 14, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ReqID:               "test-123",
					HttpCode:            testutil.Uint16Ptr(200),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Exact match - zero values should be output as "0" not "-"
			expectedLine := "- test-bucket [28/Oct/2025:14:00:00 +0000] - - test-123 - - - 200 - 0 0 0 0 - - - - - - - - - - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should handle multiple records", func() {
			startTime := time.Date(2025, 10, 28, 15, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ReqID:               "req-1",
					Operation:           testutil.StrPtr("GET"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
				{
					BucketName:          "test-bucket",
					StartTime:           startTime.Add(1 * time.Second),
					ReqID:               "req-2",
					Operation:           testutil.StrPtr("PUT"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           startTime.Add(1 * time.Second),
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
				{
					BucketName:          "test-bucket",
					StartTime:           startTime.Add(2 * time.Second),
					ReqID:               "req-3",
					Operation:           testutil.StrPtr("DELETE"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           startTime.Add(2 * time.Second),
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")

			// Should have 3 lines (one per record)
			Expect(lines).To(HaveLen(3))

			expectedLine1 := "- test-bucket [28/Oct/2025:15:00:00 +0000] - - req-1 GET - - 0 - 0 0 0 0 - - - - - - - - - - -"
			expectedLine2 := "- test-bucket [28/Oct/2025:15:00:01 +0000] - - req-2 PUT - - 0 - 0 0 0 0 - - - - - - - - - - -"
			expectedLine3 := "- test-bucket [28/Oct/2025:15:00:02 +0000] - - req-3 DELETE - - 0 - 0 0 0 0 - - - - - - - - - - -"

			Expect(lines[0]).To(Equal(expectedLine1))
			Expect(lines[1]).To(Equal(expectedLine2))
			Expect(lines[2]).To(Equal(expectedLine3))
		})

		It("should produce valid AWS S3 Server Access Log format", func() {
			// Test with a complete record to ensure the format matches AWS specification
			startTime := time.Date(2025, 10, 28, 14, 30, 45, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketOwner:         testutil.StrPtr("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"),
					BucketName:          "my-bucket",
					StartTime:           startTime,
					Timestamp:           startTime,
					ClientIP:            testutil.StrPtr("192.0.2.3"),
					Requester:           testutil.StrPtr("79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be"),
					ReqID:               "3E57427F3EXAMPLE",
					Operation:           testutil.StrPtr("REST.GET.VERSIONING"),
					ObjectKey:           testutil.StrPtr(""),
					RequestURI:          testutil.StrPtr("GET /my-bucket?versioning HTTP/1.1"),
					HttpCode:            testutil.Uint16Ptr(200),
					ErrorCode:           testutil.StrPtr(""),
					BytesSent:           testutil.Uint64Ptr(113),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(7),
					TurnAroundTime:      testutil.Float32Ptr(4),
					Referer:             testutil.StrPtr(""),
					UserAgent:           testutil.StrPtr("S3Console/0.4"),
					VersionID:           testutil.StrPtr(""),
					SignatureVersion:    testutil.StrPtr("SigV4"),
					CipherSuite:         testutil.StrPtr("ECDHE-RSA-AES128-GCM-SHA256"),
					AuthenticationType:  testutil.StrPtr("AuthHeader"),
					HostHeader:          testutil.StrPtr("my-bucket.s3.amazonaws.com"),
					TlsVersion:          testutil.StrPtr("TLSv1.2"),
					AclRequired:         testutil.StrPtr("-"),
					LoggingTargetPrefix: "logs/",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")
			Expect(lines).To(HaveLen(1))

			expectedLine := "79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be my-bucket [28/Oct/2025:14:30:45 +0000] 192.0.2.3 79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be 3E57427F3EXAMPLE REST.GET.VERSIONING - \"GET /my-bucket?versioning HTTP/1.1\" 200 - 113 0 7 4 - \"S3Console/0.4\" - - SigV4 ECDHE-RSA-AES128-GCM-SHA256 AuthHeader my-bucket.s3.amazonaws.com TLSv1.2 - -"
			Expect(lines[0]).To(Equal(expectedLine))
		})

		It("should output dash for NULL string fields", func() {
			startTime := time.Date(2025, 10, 28, 10, 0, 0, 0, time.UTC)
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           startTime,
					ReqID:               "ABC123",
					Operation:           nil,
					ErrorCode:           nil,
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           startTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			Expect(content).To(ContainSubstring("ABC123 - -")) // ReqID Operation(NULL) ObjectKey(NULL)
		})

		It("should distinguish NULL from zero for numeric fields", func() {
			startTime := time.Date(2025, 10, 28, 10, 0, 0, 0, time.UTC)

			recordNULL := logcourier.LogRecord{
				BucketName:          "test-bucket",
				StartTime:           startTime,
				ReqID:               "req-null",
				HttpCode:            nil,
				BytesSent:           nil,
				ObjectSize:          nil,
				TotalTime:           nil,
				TurnAroundTime:      nil,
				Timestamp:           startTime,
				LoggingTargetPrefix: "",
				InsertedAt:          time.Now(),
				RaftSessionID:       0,
			}

			recordZero := logcourier.LogRecord{
				BucketName:          "test-bucket",
				StartTime:           startTime.Add(1 * time.Second),
				ReqID:               "req-zero",
				HttpCode:            testutil.Uint16Ptr(0),
				BytesSent:           testutil.Uint64Ptr(0),
				ObjectSize:          testutil.Uint64Ptr(0),
				TotalTime:           testutil.Float32Ptr(0.0),
				TurnAroundTime:      testutil.Float32Ptr(0.0),
				Timestamp:           startTime.Add(1 * time.Second),
				LoggingTargetPrefix: "",
				InsertedAt:          time.Now(),
				RaftSessionID:       0,
			}

			obj, err := builder.Build([]logcourier.LogRecord{recordNULL, recordZero})
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")
			Expect(lines).To(HaveLen(2))

			Expect(lines[0]).To(ContainSubstring("- - - - - -")) // HttpCode(NULL), ErrorCode(NULL), BytesSent(NULL), ObjectSize(NULL), TotalTime(NULL), TurnAroundTime(NULL)
			Expect(lines[1]).To(ContainSubstring("0 - 0 0 0 0")) // HttpCode=0, ErrorCode(NULL), BytesSent=0, ObjectSize=0, TotalTime=0, TurnAroundTime=0
		})

		It("should sort by StartTime with millisecond precision", func() {
			// Test that records with same second but different milliseconds are ordered correctly
			baseTime := time.Date(2025, 10, 28, 15, 30, 45, 0, time.UTC)

			// Create records with millisecond differences
			// Note: Add them in reverse chronological order to verify sorting works
			records := []logcourier.LogRecord{
				{
					BucketName:          "test-bucket",
					StartTime:           baseTime.Add(500 * time.Millisecond), // 15:30:45.500
					ReqID:               "req-3",
					Operation:           testutil.StrPtr("DELETE"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           baseTime.Add(500 * time.Millisecond),
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
				{
					BucketName:          "test-bucket",
					StartTime:           baseTime.Add(200 * time.Millisecond), // 15:30:45.200
					ReqID:               "req-2",
					Operation:           testutil.StrPtr("PUT"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           baseTime.Add(200 * time.Millisecond),
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
				{
					BucketName:          "test-bucket",
					StartTime:           baseTime, // 15:30:45.000
					ReqID:               "req-1",
					Operation:           testutil.StrPtr("GET"),
					HttpCode:            testutil.Uint16Ptr(0),
					BytesSent:           testutil.Uint64Ptr(0),
					ObjectSize:          testutil.Uint64Ptr(0),
					TotalTime:           testutil.Float32Ptr(0),
					TurnAroundTime:      testutil.Float32Ptr(0),
					Timestamp:           baseTime,
					LoggingTargetPrefix: "",
					InsertedAt:          time.Now(),
					RaftSessionID:       0,
				},
			}

			obj, err := builder.Build(records)
			Expect(err).NotTo(HaveOccurred())

			content := string(obj.Content)
			lines := strings.Split(strings.TrimSpace(content), "\n")
			Expect(lines).To(HaveLen(3))

			// Verify records are sorted chronologically by StartTime
			// All should show same second in output (AWS format has second precision)
			// but should be ordered by the millisecond-precision StartTime field
			Expect(lines[0]).To(ContainSubstring("req-1")) // First: 15:30:45.000
			Expect(lines[0]).To(ContainSubstring("GET"))
			Expect(lines[1]).To(ContainSubstring("req-2")) // Second: 15:30:45.200
			Expect(lines[1]).To(ContainSubstring("PUT"))
			Expect(lines[2]).To(ContainSubstring("req-3")) // Third: 15:30:45.500
			Expect(lines[2]).To(ContainSubstring("DELETE"))
		})
	})
})
