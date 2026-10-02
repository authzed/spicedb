//go:build datastore && crdb

package pool

import (
	"context"
	"testing"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestBorrowedPoolReleasesAfterPanic(t *testing.T) {
	engine := testdatastore.RunDatastoreEngine(t, "cockroachdb")
	engine.NewDatastore(t, func(_, uri string) datastore.Datastore {
		ctx := t.Context()
		pool, err := pgxpool.New(ctx, uri)
		require.NoError(t, err)
		defer pool.Close()
		borrowed := NewBorrowedRetryPool(pool, 1, func(*pgx.Conn) {})
		var acquired *pgxpool.Conn
		borrowed.pool = &TestPool{acquireFunc: func(ctx context.Context) (*pgxpool.Conn, error) {
			var err error
			acquired, err = pool.Acquire(ctx)
			return acquired, err
		}}
		// Retain the lease solely so a failing regression cannot hang test cleanup.
		defer func() {
			if acquired != nil {
				acquired.Release()
			}
		}()
		require.Panics(t, func() {
			_ = borrowed.BeginFunc(ctx, func(pgx.Tx) error { panic("callback panic") })
		})
		require.Zero(t, pool.Stat().AcquiredConns())
		return nil
	})
}
