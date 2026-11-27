//go:build datastore && postgres

package postgres

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// BenchmarkHeadRevision compares HeadRevision derived from pg_current_snapshot() with
// HeadRevision derived from the transaction table, under the conditions that change the
// cost of the latter: the number of open transactions on the instance (the length of the
// snapshot's in-progress list) and a long-running transaction holding the snapshot xmin
// back across many later journal rows.
func BenchmarkHeadRevision(b *testing.B) {
	engine := testdatastore.RunPostgresForTesting(b, postgresConfig.pgVersion, postgresConfig.pgbouncer)

	type shape struct {
		name            string
		openForeignTxs  int
		journalRowsHeld int
	}
	shapes := []shape{
		{name: "idle", openForeignTxs: 0, journalRowsHeld: 0},
		{name: "open-foreign-txs=50", openForeignTxs: 50, journalRowsHeld: 0},
		{name: "open-foreign-txs=200", openForeignTxs: 200, journalRowsHeld: 0},
		{name: "long-tx-over-journal-rows=10000", openForeignTxs: 1, journalRowsHeld: 10_000},
	}

	for _, stableHeadRevision := range []bool{false, true} {
		for _, sh := range shapes {
			b.Run(fmt.Sprintf("stable-head-revision=%t/%s", stableHeadRevision, sh.name), func(b *testing.B) {
				ctx := b.Context()
				var dbURI string
				ds := engine.NewDatastore(b, func(_, uri string) datastore.Datastore {
					dbURI = uri
					ds, err := newPostgresDatastore(ctx, uri, primaryInstanceID,
						RevisionQuantization(0),
						WithRevisionHeartbeat(false),
						WithStableHeadRevision(stableHeadRevision),
					)
					require.NoError(b, err)
					return ds
				})
				pds := ds.(*pgDatastore)

				writeJournalRows(b, pds, 1_000)

				foreign := openForeignTransactions(b, dbURI, sh.openForeignTxs)
				defer func() {
					for _, conn := range foreign {
						_ = conn.Close(ctx)
					}
				}()
				writeJournalRows(b, pds, sh.journalRowsHeld)

				b.ResetTimer()
				for b.Loop() {
					_, err := ds.HeadRevision(ctx)
					require.NoError(b, err)
				}
			})
		}
	}
}

func writeJournalRows(b *testing.B, pds *pgDatastore, count int) {
	b.Helper()
	for range count {
		_, err := pds.ReadWriteTx(b.Context(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return nil
		})
		require.NoError(b, err)
	}
}

// openForeignTransactions opens count connections, each holding a transaction that has
// been assigned an xid, so that they appear in every later snapshot's in-progress list.
func openForeignTransactions(b *testing.B, uri string, count int) []*pgx.Conn {
	b.Helper()
	conns := make([]*pgx.Conn, 0, count)
	for range count {
		conn, err := pgx.Connect(b.Context(), uri)
		require.NoError(b, err)
		_, err = conn.Exec(b.Context(), `BEGIN; SELECT pg_current_xact_id()`)
		require.NoError(b, err)
		conns = append(conns, conn)
	}
	return conns
}
