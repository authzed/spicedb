package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// indexForRelationTupleTransaction adds a missing index to relation_tuple_transaction table
// to support garbage collection. This is in support of the query for selecting the most recent
// transaction: `SELECT xid, snapshot FROM relation_tuple_transaction WHERE timestamp < $1 ORDER BY xid DESC LIMIT 1`
//
// EXPLAIN before the index:
// Limit  (cost=0.56..1.78 rows=1 width=558) (actual time=5706.155..5706.156 rows=1 loops=1)
// ->  Index Scan Backward using pk_rttx on relation_tuple_transaction  (cost=0.56..30428800.04 rows=25023202 width=558) (actual time=5706.154..5706.155 rows=1 loops=1)
//
//	Filter: ("timestamp" < (now() - '04:00:00'::interval))
//	Rows Removed by Filter: 6638121
//
// Planning Time: 0.098 ms
// Execution Time: 5706.192 ms
// (6 rows)
const indexForRelationTupleTransaction = `
	relation_tuple_transaction (xid DESC, timestamp);`

func init() {
	if err := DatabaseMigrations.Register("add-index-for-transaction-gc", "add-expiration-cleanup-index",
		func(ctx context.Context, conn *pgx.Conn) error {
			return createIndexConcurrently(ctx, conn, "ix_relation_tuple_transaction_xid_desc_timestamp", indexForRelationTupleTransaction)
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
