//go:build ci
// +build ci

package crdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

const (
	testUserNamespace = "test/user"
)

var testUserNS = namespace.Namespace(testUserNamespace)

func executeWithErrors(errors *[]error, maxRetries uint8) executeTxRetryFunc {
	return func(ctx context.Context, fn innerFunc) (err error) {
		wrappedFn := func(ctx context.Context) error {
			if len(*errors) > 0 {
				retErr := (*errors)[0]
				(*errors) = (*errors)[1:]
				return retErr
			}

			return fn(ctx)
		}

		return executeWithResets(ctx, wrappedFn, maxRetries)
	}
}

func TestTxReset(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "")

	cases := []struct {
		name        string
		maxRetries  uint8
		errors      []error
		expectError bool
	}{
		{
			name:       "retryable",
			maxRetries: 4,
			errors: []error{
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbRetryErrCode},
			},
			expectError: false,
		},
		{
			name:       "resettable",
			maxRetries: 4,
			errors: []error{
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
				&pgconn.PgError{Code: crdbServerNotAcceptingClients},
			},
			expectError: false,
		},
		{
			name:       "mixed",
			maxRetries: 50,
			errors: []error{
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
				&pgconn.PgError{Code: crdbRetryErrCode},
			},
			expectError: false,
		},
		{
			name:        "noErrors",
			maxRetries:  50,
			errors:      []error{},
			expectError: false,
		},
		{
			name:       "nonRecoverable",
			maxRetries: 1,
			errors: []error{
				&pgconn.PgError{Code: crdbRetryErrCode},
				&pgconn.PgError{Code: crdbAmbiguousErrorCode},
			},
			expectError: true,
		},
		{
			name:       "stale connections",
			maxRetries: 3,
			errors: []error{
				errors.New("unexpected EOF"),
				errors.New("broken pipe"),
			},
			expectError: false,
		},
		{
			name:       "clockSkew",
			maxRetries: 1,
			errors: []error{
				&pgconn.PgError{Code: crdbUnknownSQLState, Message: crdbClockSkewMessage},
			},
			expectError: false,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)

			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
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
			revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				return rwt.WriteNamespaces(testUserNS)
			})
			if tt.expectError {
				require.Error(err)
				require.Equal(datastore.NoRevision, revision)
			} else {
				require.NoError(err)
				require.True(revision.GreaterThan(decimal.Zero))
			}
		})
	}
}
