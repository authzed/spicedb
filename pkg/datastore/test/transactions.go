package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
)

// RetryTest asserts that ReadWriteTx retries a transaction whose callback
// returns retryErr, and only then.
//
// retryErr must be an error the engine under test classifies as retryable;
// there is no engine-agnostic value for it, since every engine recognizes its
// own native error (a sqlstate, a MySQL error number, a gRPC status).
func RetryTest(t *testing.T, tester DatastoreTester, retryErr error) {
	require.Error(t, retryErr, "retryErr must be a non-nil error the engine treats as retryable")

	disableRetriesOptions := []options.RWTOptionsOption{options.WithDisableRetries(true)}

	testCases := []struct {
		name                 string
		returnRetryableError bool
		txOptions            []options.RWTOptionsOption
		countAssertion       func(require.TestingT, any, ...any)
	}{
		{"retryable with retries", true, nil, require.Positive},
		{"non-retryable with retries", false, nil, require.Zero},
		{"retryable retries disabled", true, disableRetriesOptions, require.Zero},
		{"non-retryable retries disabled", false, disableRetriesOptions, require.Zero},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			ds, err := tester.New(t, DefaultRevisionParameters(), 1)
			require.NoError(err)

			ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
			defer cancel()

			var attempts int
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				attempts++

				if tc.returnRetryableError {
					return retryErr
				}
				return errors.New("not retryable")
			}, tc.txOptions...)

			require.GreaterOrEqual(attempts, 1)
			require.Error(err)

			retries := attempts - 1
			tc.countAssertion(t, retries)
		})
	}
}
