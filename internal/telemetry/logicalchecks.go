package telemetry

import "sync/atomic"

var logicalChecksCountTotal atomic.Uint64

// RecordLogicalChecks records the number of logical checks performed by the server.
func RecordLogicalChecks(logicalCheckCount uint64) {
	logicalChecksCountTotal.Add(logicalCheckCount)
}

// loadLogicalChecksCount returns the total number of logical checks performed by the server,
// zeroing out the existing count as well.
func loadLogicalChecksCount() uint64 {
	return logicalChecksCountTotal.Swap(0)
}
