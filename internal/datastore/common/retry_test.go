package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRetryBackoffIsBounded asserts that no single retry waits longer than
// MaxRetryBackoff. Before this bound existed the wait doubled without limit, so
// a retry loop configured with a large --datastore-max-tx-retries would sleep
// for hours and the caller would see a request that never returns rather than a
// retryable error.
func TestRetryBackoffIsBounded(t *testing.T) {
	t.Parallel()

	for retries := range uint8(64) {
		after := retryBackoff(context.Background(), uint(retries)+1)
		require.Positive(t, after, "retry %d waited for no time at all", retries+1)
		require.LessOrEqual(t, after, MaxRetryBackoff,
			"retry %d waited %s, longer than the %s cap", retries+1, after, MaxRetryBackoff)
	}
}

// TestRetryBackoffGrowsThenSaturates asserts the schedule still backs off
// exponentially over the range a default-configured datastore uses, so that
// bounding the tail does not turn the backoff into a tight loop.
func TestRetryBackoffGrowsThenSaturates(t *testing.T) {
	t.Parallel()

	// The jittered value stays within +/- retryBackoffJitter of the nominal
	// 25ms * 2^retries, until the cap clamps it.
	for retries := range uint8(9) {
		nominal := time.Duration(1<<retries) * retryBackoffScalar
		require.Less(t, nominal, MaxRetryBackoff, "test expects retry %d to be below the cap", retries+1)

		after := retryBackoff(context.Background(), uint(retries)+1)
		require.GreaterOrEqual(t, after, time.Duration(float64(nominal)*(1-retryBackoffJitter)))
		require.LessOrEqual(t, after, time.Duration(float64(nominal)*(1+retryBackoffJitter)))
	}
}

// TestRetryBudgetGrowsLinearly asserts that the worst-case time a retry loop can
// spend sleeping is linear in the configured retry count. This is the property
// that keeps a raised --datastore-max-tx-retries from becoming an unbounded hang.
func TestRetryBudgetGrowsLinearly(t *testing.T) {
	t.Parallel()

	for _, maxRetries := range []uint8{10, 20, 50, 100, 255} {
		var worstCase time.Duration
		for retries := range maxRetries {
			worstCase += retryBackoff(context.Background(), uint(retries)+1)
		}
		require.LessOrEqual(t, worstCase, time.Duration(maxRetries)*MaxRetryBackoff,
			"%d retries could sleep for %s", maxRetries, worstCase)
	}
}

// TestSleepOnErrStopsWhenContextIsDone asserts that a cancelled caller is not
// kept waiting out the backoff.
func TestSleepOnErrStopsWhenContextIsDone(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	// A high retry count would otherwise sleep for the full MaxRetryBackoff.
	SleepOnErr(ctx, context.Canceled, 60)
	require.Less(t, time.Since(start), MaxRetryBackoff/2)
}
