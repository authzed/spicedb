package telemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordLogicalChecks(t *testing.T) {
	// Reset the counter to ensure clean state
	logicalChecksCountTotal.Store(0)

	// Test recording a single value
	RecordLogicalChecks(5)
	count := logicalChecksCountTotal.Load()
	require.Equal(t, uint64(5), count)

	// Test recording multiple values
	RecordLogicalChecks(10)
	RecordLogicalChecks(15)
	count = logicalChecksCountTotal.Load()
	require.Equal(t, uint64(30), count)

	// Test recording zero
	RecordLogicalChecks(0)
	count = logicalChecksCountTotal.Load()
	require.Equal(t, uint64(30), count)
}

func TestLoadLogicalChecksCount(t *testing.T) {
	// Reset and set up initial state
	logicalChecksCountTotal.Store(0)
	RecordLogicalChecks(100)

	// Load should return the current value and reset to zero
	count := loadLogicalChecksCount()
	require.Equal(t, uint64(100), count)

	// After loading, the counter should be reset to zero
	currentCount := logicalChecksCountTotal.Load()
	require.Equal(t, uint64(0), currentCount)

	// Loading again should return zero
	count = loadLogicalChecksCount()
	require.Equal(t, uint64(0), count)
}

func TestLoadLogicalChecksCountWithZero(t *testing.T) {
	// Reset the counter
	logicalChecksCountTotal.Store(0)

	// Loading when counter is zero should return zero
	count := loadLogicalChecksCount()
	require.Equal(t, uint64(0), count)

	// Counter should still be zero
	currentCount := logicalChecksCountTotal.Load()
	require.Equal(t, uint64(0), currentCount)
}

func TestConcurrentLogicalChecks(t *testing.T) {
	// Reset the counter
	logicalChecksCountTotal.Store(0)

	// Simulate concurrent recording
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			RecordLogicalChecks(1)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have recorded 10 total checks
	count := logicalChecksCountTotal.Load()
	require.Equal(t, uint64(10), count)

	// Load should return 10 and reset to zero
	loadedCount := loadLogicalChecksCount()
	require.Equal(t, uint64(10), loadedCount)
	require.Equal(t, uint64(0), logicalChecksCountTotal.Load())
}
