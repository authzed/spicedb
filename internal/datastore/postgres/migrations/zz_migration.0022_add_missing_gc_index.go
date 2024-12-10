package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// addGCIndexForRelationTupleTransaction adds a missing index to relation_tuple_transaction table
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
const addGCIndexForRelationTupleTransaction = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS ix_relation_tuple_transaction_xid_desc_timestamp
	ON relation_tuple_transaction (xid DESC, timestamp);`

func init() {
	if err := DatabaseMigrations.Register("add-missing-gc-index", "add-expiration-support",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, addGCIndexForRelationTupleTransaction); err != nil {
				return fmt.Errorf("failed to add missing GC index: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
