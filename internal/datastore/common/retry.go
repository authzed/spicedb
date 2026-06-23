package common

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"

	log "github.com/authzed/spicedb/internal/logging"
)

// SleepOnErr sleeps for an exponentially increasing duration with jitter after a
// retryable error has occurred, before the caller retries the operation.
func SleepOnErr(ctx context.Context, err error, retries uint8) {
	after := retry.BackoffExponentialWithJitter(25*time.Millisecond, 0.5)(ctx, uint(retries+1)) // add one so we always wait at least a little bit
	log.Ctx(ctx).Debug().Err(err).Dur("after", after).Uint8("retry", retries+1).Msg("retrying on database error")

	select {
	case <-time.After(after):
	case <-ctx.Done():
	}
}
