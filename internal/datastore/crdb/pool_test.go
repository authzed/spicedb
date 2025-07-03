//go:build docker

package crdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

const (
	testUserNamespace = "test/user"
)

var testUserNS = namespace.Namespace(testUserNamespace)

func TestTxReset(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

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
				&pgconn.PgError{Code: pool.CrdbRetryErrCode},
				&pgconn.PgError{Code: pool.CrdbRetryErrCode},
				&pgconn.PgError{Code: pool.CrdbRetryErrCode},
			},
			expectError: false,
		},
		{
			name:       "resettable",
			maxRetries: 4,
			errors: []error{
				&pgconn.PgError{Code: pool.CrdbAmbiguousErrorCode},
				&pgconn.PgError{Code: pool.CrdbAmbiguousErrorCode},
				&pgconn.PgError{Code: pool.CrdbServerNotAcceptingClients},
			},
			expectError: false,
		},
		{
			name:       "mixed",
			maxRetries: 50,
			errors: []error{
				&pgconn.PgError{Code: pool.CrdbRetryErrCode},
				&pgconn.PgError{Code: pool.CrdbAmbiguousErrorCode},
				&pgconn.PgError{Code: pool.CrdbRetryErrCode},
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
				&pgconn.PgError{Code: pool.CrdbRetryErrCode},
				&pgconn.PgError{Code: pool.CrdbAmbiguousErrorCode},
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
				&pgconn.PgError{Code: pool.CrdbUnknownSQLState, Message: pool.CrdbClockSkewMessage},
			},
			expectError: false,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := newCRDBDatastore(
					ctx,
					uri,
					GCWindow(24*time.Hour),
					RevisionQuantization(5*time.Second),
					WatchBufferLength(128),
					MaxRetries(tt.maxRetries),
				)
				require.NoError(err)
				return ds
			})
			defer ds.Close()

			r, err := ds.ReadyState(ctx)
			require.NoError(err)
			require.True(r.IsReady)

			// WriteNamespace utilizes execute so we'll use it
			i := 0
			rev, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
				if i < len(tt.errors) {
					defer func() { i++ }()
					return tt.errors[i]
				}
				return rwt.WriteNamespaces(ctx, testUserNS)
			})
			if tt.expectError {
				require.Error(err)
				require.Equal(datastore.NoRevision, rev)
			} else {
				require.NoError(err)
				require.NotEqual(datastore.NoRevision, rev)
			}
		})
	}
}
