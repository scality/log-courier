package logcourier

import (
	"fmt"
	"time"
)

// LogBatch represents a batch of logs ready for processing
type LogBatch struct {
	MinTimestamp  time.Time
	MaxTimestamp  time.Time
	Bucket        string
	RaftSessionID uint16
	LogCount      uint64
}

// String returns a string representation for logging
func (lb LogBatch) String() string {
	return fmt.Sprintf("LogBatch{Bucket: %s, RaftSessionID: %d, Logs: %d, TimeRange: [%s, %s]}",
		lb.Bucket, lb.RaftSessionID, lb.LogCount, lb.MinTimestamp, lb.MaxTimestamp)
}
