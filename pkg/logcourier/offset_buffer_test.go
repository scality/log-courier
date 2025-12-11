package logcourier_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/testutil"
)

// MockOffsetManager is used for failure simulation tests
type MockOffsetManager struct {
	commits      []CommitCall
	shouldFail   bool
	failCount    int
	attemptCount int
}

type CommitCall struct {
	Offset        logcourier.Offset
	Bucket        string
	RaftSessionID uint16
}

func (m *MockOffsetManager) CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, offset logcourier.Offset) error {
	m.attemptCount++

	if m.shouldFail {
		return fmt.Errorf("simulated commit failure")
	}

	if m.failCount > 0 {
		m.failCount--
		return fmt.Errorf("transient failure")
	}

	m.commits = append(m.commits, CommitCall{Offset: offset, Bucket: bucket, RaftSessionID: raftSessionID})
	return nil
}

func (m *MockOffsetManager) CommitOffsetsBatch(ctx context.Context, requests []logcourier.OffsetCommitRequest) error {
	m.attemptCount++

	if m.shouldFail {
		return fmt.Errorf("simulated commit failure")
	}

	if m.failCount > 0 {
		m.failCount--
		return fmt.Errorf("transient failure")
	}

	for _, req := range requests {
		m.commits = append(m.commits, CommitCall{Offset: req.Offset, Bucket: req.Bucket, RaftSessionID: req.RaftSessionID})
	}
	return nil
}

func (m *MockOffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (logcourier.Offset, error) {
	return logcourier.Offset{}, nil
}

var _ = Describe("OffsetBuffer", func() {
	var (
		ctx           context.Context
		helper        *testutil.ClickHouseTestHelper
		offsetManager *logcourier.OffsetManager
		logger        *slog.Logger
	)

	BeforeEach(func() {
		ctx = context.Background()
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		var err error
		helper, err = testutil.NewClickHouseTestHelper(ctx)
		Expect(err).NotTo(HaveOccurred())

		err = helper.SetupSchema(ctx)
		Expect(err).NotTo(HaveOccurred())

		offsetManager = logcourier.NewOffsetManager(helper.Client, helper.DatabaseName)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("NewOffsetBuffer", func() {
		It("creates buffer with configuration", func() {
			buffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferConfig{
				MaxRetries:          3,
				InitialBackoff:      1 * time.Second,
				MaxBackoff:          10 * time.Second,
				BackoffJitterFactor: 0.2,
				OffsetManager:       offsetManager,
				Logger:              logger,
			})

			Expect(buffer).NotTo(BeNil())
		})
	})

	Describe("Put", func() {
		var buffer *logcourier.OffsetBuffer

		BeforeEach(func() {
			buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferConfig{
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       offsetManager,
				Logger:              logger,
			})
		})

		It("stores offset in buffer", func() {
			offset := logcourier.Offset{
				InsertedAt: time.Now(),
				Timestamp:  time.Now(),
				ReqID:      "req1",
			}

			buffer.Put("bucket1", 123, offset)
		})

		It("updates existing offset for same bucket/raftSessionID", func() {
			offset1 := logcourier.Offset{
				InsertedAt: time.Now(),
				Timestamp:  time.Now(),
				ReqID:      "req1",
			}
			offset2 := logcourier.Offset{
				InsertedAt: time.Now().Add(time.Second),
				Timestamp:  time.Now().Add(time.Second),
				ReqID:      "req2",
			}

			buffer.Put("bucket1", 123, offset1)
			buffer.Put("bucket1", 123, offset2)

			// Verify only one entry in buffer (tested via flush later)
		})

		It("stores multiple offsets in buffer", func() {
			offset := logcourier.Offset{
				InsertedAt: time.Now(),
				Timestamp:  time.Now(),
				ReqID:      "req1",
			}

			buffer.Put("bucket1", 123, offset)
			buffer.Put("bucket2", 456, offset)
		})
	})

	Describe("Flush", func() {
		var buffer *logcourier.OffsetBuffer

		BeforeEach(func() {
			buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferConfig{
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       offsetManager,
				Logger:              logger,
			})
		})

		It("flushes all buffered offsets to ClickHouse", func() {
			offset1 := logcourier.Offset{
				InsertedAt: time.Now().UTC().Truncate(time.Second),
				Timestamp:  time.Now().UTC().Truncate(time.Second),
				ReqID:      "req1",
			}
			offset2 := logcourier.Offset{
				InsertedAt: time.Now().Add(time.Second).UTC().Truncate(time.Second),
				Timestamp:  time.Now().Add(time.Second).UTC().Truncate(time.Second),
				ReqID:      "req2",
			}

			buffer.Put("bucket1", 123, offset1)
			buffer.Put("bucket2", 456, offset2)

			err := buffer.Flush(ctx)

			Expect(err).NotTo(HaveOccurred())

			// Verify offsets were committed to ClickHouse
			retrievedOffset1, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset1).To(Equal(offset1))

			retrievedOffset2, err := offsetManager.GetOffset(ctx, "bucket2", 456)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset2).To(Equal(offset2))
		})

		It("clears buffer after successful flush", func() {
			offset := logcourier.Offset{
				InsertedAt: time.Now().UTC().Truncate(time.Second),
				Timestamp:  time.Now().UTC().Truncate(time.Second),
				ReqID:      "req1",
			}

			buffer.Put("bucket1", 123, offset)
			err := buffer.Flush(ctx)
			Expect(err).NotTo(HaveOccurred())

			// Verify offset was committed
			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset))

			// Flush again - should be no-op (empty buffer)
			err = buffer.Flush(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("does nothing when buffer is empty", func() {
			err := buffer.Flush(ctx)
			Expect(err).NotTo(HaveOccurred())
		})

		It("keeps offsets in buffer on flush failure", func() {
			mockOffsetMgr := &MockOffsetManager{shouldFail: true}
			mockBuffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferConfig{
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       mockOffsetMgr,
				Logger:              logger,
			})

			offset := logcourier.Offset{
				InsertedAt: time.Now(),
				Timestamp:  time.Now(),
				ReqID:      "req1",
			}

			mockBuffer.Put("bucket1", 123, offset)
			err := mockBuffer.Flush(ctx)

			Expect(err).To(HaveOccurred())

			// Reset mock to succeed
			mockOffsetMgr.shouldFail = false
			mockOffsetMgr.commits = nil

			// Flush again - should still have offset
			err = mockBuffer.Flush(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockOffsetMgr.commits).To(HaveLen(1))
		})

		It("retries flush on transient errors", func() {
			mockOffsetMgr := &MockOffsetManager{failCount: 2}
			mockBuffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferConfig{
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       mockOffsetMgr,
				Logger:              logger,
			})

			offset := logcourier.Offset{
				InsertedAt: time.Now(),
				Timestamp:  time.Now(),
				ReqID:      "req1",
			}

			mockBuffer.Put("bucket1", 123, offset)
			err := mockBuffer.Flush(ctx)

			Expect(err).NotTo(HaveOccurred())
			Expect(mockOffsetMgr.commits).To(HaveLen(1)) // Eventually succeeded
			Expect(mockOffsetMgr.attemptCount).To(Equal(3)) // 2 failures + 1 success
		})

		It("keeps offsets on exhausted retries", func() {
			mockOffsetMgr := &MockOffsetManager{failCount: 100}
			mockBuffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferConfig{
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       mockOffsetMgr,
				Logger:              logger,
			})

			offset := logcourier.Offset{
				InsertedAt: time.Now(),
				Timestamp:  time.Now(),
				ReqID:      "req1",
			}

			mockBuffer.Put("bucket1", 123, offset)
			err := mockBuffer.Flush(ctx)

			Expect(err).To(HaveOccurred())

			// Reset mock to succeed
			mockOffsetMgr.failCount = 0
			mockOffsetMgr.commits = nil
			mockOffsetMgr.attemptCount = 0

			// Retry should work
			err = mockBuffer.Flush(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(mockOffsetMgr.commits).To(HaveLen(1))
		})
	})
})
