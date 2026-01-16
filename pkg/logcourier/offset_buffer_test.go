package logcourier_test

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scality/log-courier/pkg/logcourier"
	"github.com/scality/log-courier/pkg/testutil"
)

// mockOffsetManager is a test helper that combines multiple testing features:
// - Delay: adds artificial latency to simulate slow ClickHouse operations
// - Counting: tracks the number of commit operations
// - Interception: runs a callback during commit to simulate race conditions
//
//nolint:govet // fieldalignment: struct size cannot be reduced further
type mockOffsetManager struct {
	commitCount atomic.Int64 // Must be first for proper alignment
	real        logcourier.OffsetManagerInterface
	delay       time.Duration
	onCommit    func()
}

func (m *mockOffsetManager) CommitOffset(ctx context.Context, bucket string, raftSessionID uint16, offset logcourier.Offset) error {
	m.commitCount.Add(1)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.onCommit != nil {
		m.onCommit()
	}
	return m.real.CommitOffset(ctx, bucket, raftSessionID, offset)
}

func (m *mockOffsetManager) CommitOffsetsBatch(ctx context.Context, commits []logcourier.OffsetCommit) error {
	m.commitCount.Add(1)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	if m.onCommit != nil {
		m.onCommit()
	}
	return m.real.CommitOffsetsBatch(ctx, commits)
}

func (m *mockOffsetManager) GetOffset(ctx context.Context, bucket string, raftSessionID uint16) (logcourier.Offset, error) {
	return m.real.GetOffset(ctx, bucket, raftSessionID)
}

func (m *mockOffsetManager) GetCommitCount() int64 {
	return m.commitCount.Load()
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

		offsetManager = logcourier.NewOffsetManager(helper.Client(), helper.DatabaseName)
	})

	AfterEach(func() {
		if helper != nil {
			_ = helper.TeardownSchema(ctx)
			_ = helper.Close()
		}
	})

	Describe("NewOffsetBuffer", func() {
		It("creates buffer with configuration", func() {
			buffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
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
		var (
			buffer *logcourier.OffsetBuffer
			stop   func()
		)

		BeforeEach(func() {
			buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       offsetManager,
				Logger:              logger,
			})
			stop = buffer.Start(ctx)
		})

		AfterEach(func() {
			stop()
		})

		It("stores offset in buffer", func() {
			offset := logcourier.Offset{
				InsertedAt: time.Now().UTC().Truncate(time.Second),
				Timestamp:  time.Now().UTC().Truncate(time.Second),
				ReqID:      "req1",
			}

			buffer.Put("bucket1", 123, offset)

			// Verify offset is buffered by flushing
			err := buffer.Flush(ctx, logcourier.FlushReasonExplicit)
			Expect(err).NotTo(HaveOccurred())

			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset))
		})

		It("updates existing offset for same bucket/raftSessionID", func() {
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
			buffer.Put("bucket1", 123, offset2)

			// Flush and verify only the latest offset is committed
			err := buffer.Flush(ctx, logcourier.FlushReasonExplicit)
			Expect(err).NotTo(HaveOccurred())

			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset2), "Should have latest offset, not first one")
		})

		It("stores multiple offsets in buffer", func() {
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

			// Verify both offsets are buffered by flushing
			err := buffer.Flush(ctx, logcourier.FlushReasonExplicit)
			Expect(err).NotTo(HaveOccurred())

			retrievedOffset1, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset1).To(Equal(offset1))

			retrievedOffset2, err := offsetManager.GetOffset(ctx, "bucket2", 456)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset2).To(Equal(offset2))
		})
	})

	Describe("Flush", func() {
		It("retries flush on transient errors", func() {
			failingOffsetMgr := testutil.NewFailingOffsetManager(offsetManager, 2) // Fail first 2 attempts
			failingBuffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       failingOffsetMgr,
				Logger:              logger,
			})
			stop := failingBuffer.Start(ctx)
			defer stop()

			offset := logcourier.Offset{
				InsertedAt: time.Now().UTC().Truncate(time.Second),
				Timestamp:  time.Now().UTC().Truncate(time.Second),
				ReqID:      "req1",
			}

			failingBuffer.Put("bucket1", 123, offset)
			err := failingBuffer.Flush(ctx, logcourier.FlushReasonExplicit)

			// Should succeed after retries
			Expect(err).NotTo(HaveOccurred())

			// Verify offset was committed
			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset))

			// Verify 3 attempts were made (2 failures + 1 success)
			Expect(failingOffsetMgr.GetCommitCount()).To(Equal(int64(3)))
		})

		It("keeps offsets in buffer when retries are exhausted", func() {
			failingOffsetMgr := testutil.NewFailingOffsetManager(offsetManager, 100) // Fail all attempts
			failingBuffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				MaxRetries:          1,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       failingOffsetMgr,
				Logger:              logger,
			})
			stop1 := failingBuffer.Start(ctx)
			defer stop1()

			offset := logcourier.Offset{
				InsertedAt: time.Now().UTC().Truncate(time.Second),
				Timestamp:  time.Now().UTC().Truncate(time.Second),
				ReqID:      "req1",
			}

			failingBuffer.Put("bucket1", 123, offset)
			err := failingBuffer.Flush(ctx, logcourier.FlushReasonExplicit)

			// Should fail after exhausting retries
			Expect(err).To(HaveOccurred())

			// Verify offset was not committed
			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset.InsertedAt.IsZero()).To(BeTrue(), "Offset should not be committed after exhausted retries")

			// Create new buffer with working offset manager and retry
			workingBuffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				OffsetManager:       offsetManager,
				Logger:              logger,
			})
			stop2 := workingBuffer.Start(ctx)
			defer stop2()

			// Put same offset and flush
			workingBuffer.Put("bucket1", 123, offset)
			err = workingBuffer.Flush(ctx, logcourier.FlushReasonExplicit)
			Expect(err).NotTo(HaveOccurred())

			// Verify offset was committed
			retrievedOffset, err = offsetManager.GetOffset(ctx, "bucket1", 123)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset))
		})
	})

	Describe("OffsetBuffer Lifecycle", func() {
		var (
			buffer     *logcourier.OffsetBuffer
			testLogger *slog.Logger
		)

		BeforeEach(func() {
			testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelError,
			}))

			buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				OffsetManager:       offsetManager,
				Logger:              testLogger,
				MaxRetries:          3,
				InitialBackoff:      100 * time.Millisecond,
				MaxBackoff:          1 * time.Second,
				BackoffJitterFactor: 0.1,
				FlushTimeThreshold:  100 * time.Millisecond,
				FlushCountThreshold: 5,
			})
		})

		Describe("Stop function", func() {
			It("should shutdown the flush loop and flush remaining offsets", func() {
				stop := buffer.Start(ctx)

				// Give goroutine time to start
				time.Sleep(10 * time.Millisecond)

				// Add an offset to buffer
				offset := logcourier.Offset{
					InsertedAt: time.Now().UTC().Truncate(time.Second),
					Timestamp:  time.Now().UTC().Truncate(time.Second),
					ReqID:      "req1",
				}
				buffer.Put("bucket1", 1, offset)

				// Call stop - flushLoop should shutdown and flush remaining offsets
				stop()

				// Verify the offset was flushed during shutdown
				retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedOffset).To(Equal(offset), "Offset should be flushed during shutdown")
			})
		})
	})

	Describe("OffsetBuffer Flushing", func() {
		var (
			buffer     *logcourier.OffsetBuffer
			testLogger *slog.Logger
			stop       func()
		)

		BeforeEach(func() {
			testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelError,
			}))

			buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				OffsetManager:       offsetManager,
				Logger:              testLogger,
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.1,
				FlushTimeThreshold:  1 * time.Second,  // Long enough to not trigger during test
				FlushCountThreshold: 3,                // Trigger after 3 offsets
			})
			stop = buffer.Start(ctx)
		})

		AfterEach(func() {
			stop()
		})

		Describe("Count-based flushing", func() {
			It("should flush when count threshold is reached", func() {
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
				offset3 := logcourier.Offset{
					InsertedAt: time.Now().Add(2 * time.Second).UTC().Truncate(time.Second),
					Timestamp:  time.Now().Add(2 * time.Second).UTC().Truncate(time.Second),
					ReqID:      "req3",
				}

				// Add offsets up to threshold
				buffer.Put("bucket1", 1, offset1)
				buffer.Put("bucket2", 1, offset2)

				// Should not have flushed yet
				retrievedOffset1, _ := offsetManager.GetOffset(ctx, "bucket1", 1)
				Expect(retrievedOffset1.InsertedAt.IsZero()).To(BeTrue(), "bucket1 should not be committed yet")

				// Add one more to hit threshold
				buffer.Put("bucket3", 1, offset3)

				// Should trigger flush asynchronously - verify all 3 offsets in ClickHouse
				Eventually(func() bool {
					offset1Check, _ := offsetManager.GetOffset(ctx, "bucket1", 1)
					offset2Check, _ := offsetManager.GetOffset(ctx, "bucket2", 1)
					offset3Check, _ := offsetManager.GetOffset(ctx, "bucket3", 1)
					return !offset1Check.InsertedAt.IsZero() &&
						!offset2Check.InsertedAt.IsZero() &&
						!offset3Check.InsertedAt.IsZero()
				}, 2*time.Second, 100*time.Millisecond).Should(BeTrue(), "All 3 offsets should be flushed")

				// Verify offset values
				retrievedOffset1, err := offsetManager.GetOffset(ctx, "bucket1", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedOffset1).To(Equal(offset1))

				retrievedOffset2, err := offsetManager.GetOffset(ctx, "bucket2", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedOffset2).To(Equal(offset2))

				retrievedOffset3, err := offsetManager.GetOffset(ctx, "bucket3", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedOffset3).To(Equal(offset3))
			})

			It("should not flush when count threshold is not reached", func() {
				offset := logcourier.Offset{
					InsertedAt: time.Now().UTC().Truncate(time.Second),
					Timestamp:  time.Now().UTC().Truncate(time.Second),
					ReqID:      "req1",
				}

				// Add offsets below threshold
				buffer.Put("bucket1", 1, offset)

				// Wait a bit to ensure no async flush happens
				time.Sleep(200 * time.Millisecond)

				// Should not have flushed
				retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedOffset.InsertedAt.IsZero()).To(BeTrue(), "Offset should not be flushed yet")
			})
		})

		Describe("Time-based flushing", func() {
			It("should flush when time threshold is reached", func() {
				// Use short time threshold for test
				stop()
				buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
					Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
					OffsetManager:       offsetManager,
					Logger:              testLogger,
					MaxRetries:          3,
					InitialBackoff:      10 * time.Millisecond,
					MaxBackoff:          100 * time.Millisecond,
					BackoffJitterFactor: 0.1,
					FlushTimeThreshold:  200 * time.Millisecond,
					FlushCountThreshold: 100, // High enough to not trigger during test
				})
				stop = buffer.Start(ctx)

				offset := logcourier.Offset{
					InsertedAt: time.Now().UTC().Truncate(time.Second),
					Timestamp:  time.Now().UTC().Truncate(time.Second),
					ReqID:      "req1",
				}

				// Add offset below count threshold
				buffer.Put("bucket1", 1, offset)

				// Should not flush immediately
				initialOffset, _ := offsetManager.GetOffset(ctx, "bucket1", 1)
				Expect(initialOffset.InsertedAt.IsZero()).To(BeTrue(), "Offset should not be flushed immediately")

				// Wait for time threshold and verify offset appears in ClickHouse
				Eventually(func() bool {
					checkOffset, _ := offsetManager.GetOffset(ctx, "bucket1", 1)
					return !checkOffset.InsertedAt.IsZero()
				}, 1*time.Second, 50*time.Millisecond).Should(BeTrue(), "Offset should be flushed after time threshold")

				// Verify exact offset value
				retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(retrievedOffset).To(Equal(offset))
			})

			It("should not flush when buffer is empty", func() {
				// Use failing offset manager to track flush attempts
				stop()
				failingOffsetMgr := testutil.NewFailingOffsetManager(offsetManager, 0) // Don't fail, just count
				buffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
					Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
					OffsetManager:       failingOffsetMgr,
					Logger:              testLogger,
					MaxRetries:          3,
					InitialBackoff:      10 * time.Millisecond,
					MaxBackoff:          100 * time.Millisecond,
					BackoffJitterFactor: 0.1,
					FlushTimeThreshold:  100 * time.Millisecond,
					FlushCountThreshold: 100,
				})
				stop = buffer.Start(ctx)

				// Wait for multiple time thresholds to pass
				time.Sleep(300 * time.Millisecond)

				// Verify no flush attempts were made (buffer was empty)
				Expect(failingOffsetMgr.GetCommitCount()).To(Equal(int64(0)), "Should not attempt to flush empty buffer")
			})
		})
	})

	Describe("Concurrent Flush Safety", func() {
		var testLogger *slog.Logger

		BeforeEach(func() {
			testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelError,
			}))
		})

		It("should preserve newer offset when Put during flush", func() {
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

			// Create buffer first so callback can reference it
			var mockBuffer *logcourier.OffsetBuffer

			// Use mock with interception to inject Put() during flush
			mockMgr := &mockOffsetManager{
				real: offsetManager,
				onCommit: func() {
					// This runs during flushBatch, while flush is in progress
					// Put() will send offset2 to channel, which will be received
					// by flushLoop after the current flush completes
					mockBuffer.Put("bucket1", 1, offset2)
				},
			}
			mockBuffer = logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				OffsetManager:       mockMgr,
				Logger:              testLogger,
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.1,
			})
			stop := mockBuffer.Start(ctx)
			defer stop()

			// Put offset1 and flush
			mockBuffer.Put("bucket1", 1, offset1)

			// Flush will:
			// 1. flushLoop receives flush request
			// 2. Snapshot {offset1}
			// 3. Commit offset1 to ClickHouse
			// 4. onCommit hook runs: Put(offset2) sends to channel
			// 5. Clear offset1 from map
			// 6. flushLoop returns to select, receives offset2 from channel
			err := mockBuffer.Flush(ctx, logcourier.FlushReasonExplicit)
			Expect(err).NotTo(HaveOccurred())

			// Give time for offset2 to be received from channel
			time.Sleep(50 * time.Millisecond)

			// Now flush again - should flush offset2
			err = mockBuffer.Flush(ctx, logcourier.FlushReasonExplicit)
			Expect(err).NotTo(HaveOccurred())

			// Verify both offsets were committed and offset2 is the latest
			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset2), "Should have preserved and flushed offset2")
		})
	})

	Describe("Cycle Boundary Flush Guarantees", func() {
		var testLogger *slog.Logger

		BeforeEach(func() {
			testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelError,
			}))
		})

		It("should wait for in-progress flush to complete", func() {
			var asyncFlushStarted atomic.Bool

			// Create a mock that delays commits to simulate slow network
			mockMgr := &mockOffsetManager{
				real:  offsetManager,
				delay: 400 * time.Millisecond, // Slow flush
				onCommit: func() {
					asyncFlushStarted.Store(true)
				},
			}

			buffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				OffsetManager:       mockMgr,
				Logger:              testLogger,
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				FlushCountThreshold: 2,              // Trigger after 2 offsets
				FlushTimeThreshold:  10 * time.Second, // Don't trigger time-based
			})
			stop := buffer.Start(ctx)
			defer stop()

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

			// Add 2 offsets to trigger first flush (async)
			buffer.Put("bucket1", 1, offset1)
			buffer.Put("bucket2", 1, offset2) // Triggers async flush

			// Wait for first flush to start
			Eventually(asyncFlushStarted.Load).WithTimeout(2 * time.Second).WithPolling(10 * time.Millisecond).Should(BeTrue())

			// While first flush is in progress, add a new offset
			offset3 := logcourier.Offset{
				InsertedAt: time.Now().Add(2 * time.Second).UTC().Truncate(time.Second),
				Timestamp:  time.Now().Add(2 * time.Second).UTC().Truncate(time.Second),
				ReqID:      "req3",
			}
			buffer.Put("bucket3", 1, offset3)

			// Give first flush time to be in progress (but not complete)
			time.Sleep(100 * time.Millisecond)

			// Call second flush while first flush is in progress
			secondFlushDone := make(chan error)
			secondFlushStarted := make(chan struct{})
			go func() {
				close(secondFlushStarted)
				secondFlushDone <- buffer.Flush(ctx, logcourier.FlushReasonExplicit)
			}()

			// Wait for second flush to be called
			<-secondFlushStarted

			// Give second flush time to reach the Wait() call
			time.Sleep(50 * time.Millisecond)

			// Verify second flush has NOT returned yet (should be waiting)
			select {
			case <-secondFlushDone:
				Fail("Second flush returned before first flush completed!")
			case <-time.After(100 * time.Millisecond):
				// Expected - second flush is still waiting
			}

			// Wait for second flush to complete
			// It should wait for first flush, then flush offset3
			var secondFlushErr error
			Eventually(func() bool {
				select {
				case secondFlushErr = <-secondFlushDone:
					return true
				default:
					return false
				}
			}).WithTimeout(2 * time.Second).WithPolling(50 * time.Millisecond).Should(BeTrue(),
				"Second flush should complete after first flush finishes")

			Expect(secondFlushErr).NotTo(HaveOccurred())

			// Verify all three offsets were committed
			for i, bucket := range []string{"bucket1", "bucket2", "bucket3"} {
				retrievedOffset, getErr := offsetManager.GetOffset(ctx, bucket, 1)
				Expect(getErr).NotTo(HaveOccurred())
				Expect(retrievedOffset.InsertedAt.IsZero()).To(BeFalse(),
					"Offset for %s should be committed", bucket)

				expectedOffset := []logcourier.Offset{offset1, offset2, offset3}[i]
				Expect(retrievedOffset).To(Equal(expectedOffset))
			}

			// Verify two DB commits happened: one for first flush (offset1, offset2),
			// one for second flush (offset3)
			Expect(mockMgr.GetCommitCount()).To(Equal(int64(2)),
				"Should have two commits: first flush and second flush")
		})

		It("should handle multiple concurrent cycle boundary flushes", func() {
			// Verify that multiple cycle boundary flushes can wait correctly

			mockMgr := &mockOffsetManager{
				real:  offsetManager,
				delay: 200 * time.Millisecond, // Moderate delay
			}

			buffer := logcourier.NewOffsetBuffer(logcourier.OffsetBufferOptions{
				Metrics:             logcourier.NewMetricsWithRegistry(prometheus.NewRegistry()),
				OffsetManager:       mockMgr,
				Logger:              testLogger,
				MaxRetries:          3,
				InitialBackoff:      10 * time.Millisecond,
				MaxBackoff:          100 * time.Millisecond,
				BackoffJitterFactor: 0.0,
				FlushCountThreshold: 0,              // Disable count-based
				FlushTimeThreshold:  10 * time.Second, // Disable time-based
			})
			stop := buffer.Start(ctx)
			defer stop()

			// Add some offsets
			offset := logcourier.Offset{
				InsertedAt: time.Now().UTC().Truncate(time.Second),
				Timestamp:  time.Now().UTC().Truncate(time.Second),
				ReqID:      "req1",
			}
			buffer.Put("bucket1", 1, offset)

			// Start multiple concurrent flushes
			flushCount := 5
			errors := make(chan error, flushCount)

			for i := 0; i < flushCount; i++ {
				go func() {
					errors <- buffer.Flush(ctx, logcourier.FlushReasonCycleBoundary)
				}()
			}

			// All flushes should complete without error
			for i := 0; i < flushCount; i++ {
				err := <-errors
				Expect(err).NotTo(HaveOccurred())
			}

			// Verify offset was committed (exactly once)
			retrievedOffset, err := offsetManager.GetOffset(ctx, "bucket1", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(retrievedOffset).To(Equal(offset))

			// Verify only one actual DB commit happened
			// (first flush does work, rest wait and find empty buffer)
			Expect(mockMgr.GetCommitCount()).To(Equal(int64(1)),
				"Should only commit once despite multiple concurrent flushes")
		})
	})
})
