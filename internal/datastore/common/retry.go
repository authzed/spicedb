package common

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"

	log "github.com/authzed/spicedb/internal/logging"
)

const (
	// retryBackoffScalar is the wait after the first retry; it doubles on each
	// subsequent one until it reaches MaxRetryBackoff.
	retryBackoffScalar = 25 * time.Millisecond

	// retryBackoffJitter randomly adjusts each wait so that transactions which
	// collided do not all retry in lockstep.
	retryBackoffJitter = 0.5

	// MaxRetryBackoff caps a single retry's wait. Uncapped, the doubling turns
	// --datastore-max-tx-retries into a wall-clock budget rather than a count of
	// attempts: 25 seconds of waiting at the default of 10, but 14 minutes at 15
	// and over seven hours at 20. 10s is the largest cap that leaves the default
	// schedule intact apart from its final wait.
	MaxRetryBackoff = 10 * time.Second
)

// retryBackoff computes the wait before the attempt numbered by its argument
// (1-based), bounded by MaxRetryBackoff.
var retryBackoff = retry.BackoffExponentialWithJitterBounded(retryBackoffScalar, retryBackoffJitter, MaxRetryBackoff)

// SleepOnErr sleeps before the caller retries an operation that failed with a
// retryable error, backing off exponentially with jitter and bounded by
// MaxRetryBackoff. It returns early if the context is cancelled.
func SleepOnErr(ctx context.Context, err error, retries uint8) {
	after := retryBackoff(ctx, uint(retries+1)) // add one so we always wait at least a little bit
	log.Ctx(ctx).Debug().Err(err).Dur("after", after).Uint8("retry", retries+1).Msg("retrying on database error")

	select {
	case <-time.After(after):
	case <-ctx.Done():
	}
}
