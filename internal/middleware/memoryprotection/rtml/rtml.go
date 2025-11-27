package rtml

import (
	"github.com/odigos-io/go-rtml"

	"github.com/authzed/spicedb/internal/middleware/memoryprotection"
)

var _ memoryprotection.MemoryUsageProvider = (*GoRealTimeMemoryLimiter)(nil)

type GoRealTimeMemoryLimiter struct{}

func (g GoRealTimeMemoryLimiter) IsMemLimitReached() bool {
	// NOTE: when https://github.com/odigos-io/go-rtml/issues/3 is implemented, change to a threshold-based approach
	// so that we can prioritize dispatch (internal) requests over external requests
	return rtml.IsMemLimitReached()
}

// NewRealTimeMemoryUsageProvider uses a 3rd party dependency that reaches into Go's internals to fetch current memory usage.
// This was decided to be better than us fetching current usage on an interval.
func NewRealTimeMemoryUsageProvider() *GoRealTimeMemoryLimiter {
	return &GoRealTimeMemoryLimiter{}
}
