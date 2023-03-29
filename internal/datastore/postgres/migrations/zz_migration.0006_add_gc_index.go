package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	createDeletedTransactionIndex = `CREATE INDEX CONCURRENTLY ix_relation_tuple_by_deleted_transaction ON relation_tuple (deleted_transaction)`
)

func init() {
	if err := DatabaseMigrations.Register("add-gc-index", "change-transaction-timestamp-default",
		func(ctx context.Context, conn *pgx.Conn) error {
			// CREATE INDEX CONCURRENTLY cannot run inside a transaction block (SQLSTATE 25001)
			_, err := conn.Exec(ctx, createDeletedTransactionIndex)
			return err
		}, noTxMigration,
	); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
