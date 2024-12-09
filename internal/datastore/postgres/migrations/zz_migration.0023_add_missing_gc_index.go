package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const addGCIndexForRelationTupleTransaction = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS ix_relation_tuple_transaction_xid_desc_timestamp
	ON relation_tuple_transaction (xid DESC, timestamp);`

func init() {
	if err := DatabaseMigrations.Register("add-missing-gc-index", "add-gc-lock-table",
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
