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

	countTransactions := func(t *testing.T, conn *pgx.Conn) int {
		t.Helper()
		var count int
		require.NoError(t, conn.QueryRow(t.Context(), "SELECT COUNT(*) FROM "+schema.TableTransaction).Scan(&count))
		return count
	}

	// waitForNewTransactions polls on the test goroutine (a pgx.Conn is not
	// safe for concurrent use, so require.Eventually/Never would race with the
	// connection being closed in cleanup). It reports whether the transaction
	// count rose above initial within the observation window.
	waitForNewTransactions := func(t *testing.T, conn *pgx.Conn, initial int) bool {
		t.Helper()
		// The heartbeat interval is max(1s, quantization), so a few seconds
		// is enough for several heartbeat ticks.
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			if countTransactions(t, conn) > initial {
				return true
			}
			time.Sleep(100 * time.Millisecond)
		}
		return false
	}

	t.Run("read-write datastore writes heartbeat revisions", func(t *testing.T) {
		conn := newHeartbeatDatastore(t, false)
		initial := countTransactions(t, conn)
		require.True(t, waitForNewTransactions(t, conn, initial), "expected the revision heartbeat to insert transaction rows")
	})

	t.Run("read-only datastore does not write heartbeat revisions", func(t *testing.T) {
		conn := newHeartbeatDatastore(t, true)
		initial := countTransactions(t, conn)
		require.False(t, waitForNewTransactions(t, conn, initial), "read-only datastore must not run the revision heartbeat")
	})
}
