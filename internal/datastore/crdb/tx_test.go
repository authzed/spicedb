//go:build ci
// +build ci

package crdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

const (
	testUserNamespace = "test/user"
)

var testUserNS = namespace.Namespace(testUserNamespace)

func executeWithErrors(errors *[]error, maxRetries int) executeTxRetryFunc {
	return func(ctx context.Context, conn conn, txOptions pgx.TxOptions, fn transactionFn) (err error) {
		wrappedFn := func(tx pgx.Tx) error {
			if len(*errors) > 0 {
				retErr := (*errors)[0]
				(*errors) = (*errors)[1:]
				return retErr
			}

			return fn(tx)
		}

		return executeWithResets(ctx, conn, txOptions, wrappedFn, maxRetries)
	}
}

func TestTxReset(t *testing.T) {
	cases := []struct {
		name          string
		maxRetries    int
		errors        []error
		expectError   bool
		expectedError error
	}{
		{
			name:       "retryable",
			maxRetries: 4,
			errors: []error{
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbRetryErrCode},
			},
			expectError:   false,
			expectedError: nil,
		},
		{
			name:       "resettable",
			maxRetries: 4,
			errors: []error{
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
			},
			expectError:   false,
			expectedError: nil,
		},
		{
			name:       "mixed",
			maxRetries: 50,
			errors: []error{
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
				&pgconn.PgError{Code: crdbRetryErrCode},
			},
			expectError:   false,
			expectedError: nil,
		},
		{
			name:          "noErrors",
			maxRetries:    50,
			errors:        []error{},
			expectError:   false,
			expectedError: nil,
		},
		{
			name:       "nonRecoverable",
			maxRetries: 1,
			errors: []error{
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
			},
			expectError:   true,
			expectedError: errors.New(errReachedMaxRetry),
		},
		{
			name:       "clockSkew",
			maxRetries: 1,
			errors: []error{
				&pgconn.PgError{Code: crdbUnknownSQLState, Message: crdbClockSkewMessage},
			},
			expectError:   false,
			expectedError: nil,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ds := testdatastore.NewCRDBBuilder(t).NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := NewCRDBDatastore(
					uri,
					GCWindow(24*time.Hour),
					RevisionQuantization(5*time.Second),
					WatchBufferLength(128),
				)
				require.NoError(err)
				return ds
			})
			ds.(*crdbDatastore).execute = executeWithErrors(&tt.errors, tt.maxRetries)
			defer ds.Close()

			ctx := context.Background()
			ok, err := ds.IsReady(ctx)
			require.NoError(err)
			require.True(ok)

			// WriteNamespace utilizes execute so we'll use it
			revision, err := ds.WriteNamespace(ctx, testUserNS)
			if tt.expectedError != nil {
				require.Error(err)
				require.Equal(datastore.NoRevision, revision)
			} else {
				require.NoError(err)
				require.True(revision.GreaterThan(decimal.Zero))
			}
		})
	}
}
