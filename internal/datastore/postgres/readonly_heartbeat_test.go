//go:build datastore && postgres

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	datastorecfg "github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
	"github.com/authzed/spicedb/pkg/datastore"
)

// TestRevisionHeartbeatDisabledWhenReadOnly verifies that a Postgres datastore
// configured the way `spicedb serve --datastore-readonly` configures it does not
// run the revision heartbeat, which INSERTs into relation_tuple_transaction and
// fails against read-only credentials with SQLSTATE 25006.
func TestRevisionHeartbeatDisabledWhenReadOnly(t *testing.T) {
	t.Parallel()

	b := testdatastore.RunPostgresForTesting(t, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	// newHeartbeatDatastore builds the datastore through the same engine
	// constructor the CLI uses, and returns a raw connection for inspection.
	newHeartbeatDatastore := func(t *testing.T, readOnly bool) *pgx.Conn {
		t.Helper()

		var conn *pgx.Conn
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			cfg := datastorecfg.DefaultDatastoreConfig()
			cfg.Engine = Engine
			cfg.URI = uri
			cfg.ReadOnly = readOnly
			cfg.EnableRevisionHeartbeat = true
			cfg.RevisionQuantization = 100 * time.Millisecond

			ds, err := newDatastoreFromConfig(t.Context(), *cfg)
			require.NoError(t, err)

			conn, err = pgx.Connect(t.Context(), uri)
			require.NoError(t, err)
			t.Cleanup(func() { _ = conn.Close(context.Background()) })

			return ds
		})
		t.Cleanup(func() { _ = ds.Close() })

		return conn
	}

	// countTransactions is called from require.Eventually/Never condition
	// goroutines, so it reports errors instead of failing the test directly.
	countTransactions := func(ctx context.Context, conn *pgx.Conn) (int, error) {
		var count int
		err := conn.QueryRow(ctx, "SELECT COUNT(*) FROM "+schema.TableTransaction).Scan(&count)
		return count, err
	}

	// The heartbeat interval is max(1s, quantization), so a few seconds is
	// enough for several heartbeat ticks.
	const observeFor = 4 * time.Second

	t.Run("read-write datastore writes heartbeat revisions", func(t *testing.T) {
		conn := newHeartbeatDatastore(t, false)
		initial, err := countTransactions(t.Context(), conn)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			count, err := countTransactions(t.Context(), conn)
			return err == nil && count > initial
		}, observeFor, 100*time.Millisecond, "expected the revision heartbeat to insert transaction rows")
	})

	t.Run("read-only datastore does not write heartbeat revisions", func(t *testing.T) {
		conn := newHeartbeatDatastore(t, true)
		initial, err := countTransactions(t.Context(), conn)
		require.NoError(t, err)

		require.Never(t, func() bool {
			count, err := countTransactions(t.Context(), conn)
			return err != nil || count > initial
		}, observeFor, 100*time.Millisecond, "read-only datastore must not run the revision heartbeat")
	})
}
