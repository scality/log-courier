package logcourier

import (
	"fmt"
)

// LogBatch represents a batch of logs ready for processing
type LogBatch struct {
	LastProcessedOffset Offset // Offset to filter from (empty if no previous processing)
	Bucket              string
	LogCount            uint64
}

// String returns a string representation for logging
func (lb LogBatch) String() string {
	if lb.LastProcessedOffset.InsertedAt.IsZero() {
		return fmt.Sprintf("LogBatch{Bucket: %s, Logs: %d, FirstProcessing: true}",
			lb.Bucket, lb.LogCount)
	}
	return fmt.Sprintf("LogBatch{Bucket: %s, Logs: %d, AfterOffset: %s}",
		lb.Bucket, lb.LogCount, lb.LastProcessedOffset.InsertedAt)
}
