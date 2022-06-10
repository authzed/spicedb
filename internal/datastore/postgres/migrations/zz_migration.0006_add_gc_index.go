package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	createDeletedTransactionIndex = `CREATE INDEX CONCURRENTLY ix_relation_tuple_by_deleted_transaction ON relation_tuple (deleted_transaction)`
)

func init() {
	if err := DatabaseMigrations.Register("add-gc-index", "change-transaction-timestamp-default", func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, createDeletedTransactionIndex)
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
