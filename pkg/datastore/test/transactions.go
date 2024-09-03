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

func RetryTest(t *testing.T, tester DatastoreTester) {
	disableRetriesOptions := []options.RWTOptionsOption{options.WithDisableRetries(true)}

	testCases := []struct {
		name                 string
		returnRetryableError bool
		txOptions            []options.RWTOptionsOption
		countAssertion       func(require.TestingT, interface{}, ...interface{})
	}{
		{"retryable with retries", true, nil, require.Positive},
		{"non-retryable with retries", false, nil, require.Zero},
		{"retryable retries disabled", true, disableRetriesOptions, require.Zero},
		{"non-retryable retries disabled", false, disableRetriesOptions, require.Zero},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
			require.NoError(err)

			var ds TestableDatastore
			if tds, ok := rawDS.(TestableDatastore); ok {
				ds = tds
			} else {
				if uw, ok := rawDS.(datastore.UnwrappableDatastore); ok {
					ds = uw.Unwrap().(TestableDatastore)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
			defer cancel()

			var attempts int
			_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				attempts++

				if tc.returnRetryableError {
					return ds.ExampleRetryableError()
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
