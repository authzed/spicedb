package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const createIndexOnTupleTransactionTimestamp = `
	CREATE INDEX ix_relation_tuple_transaction_by_timestamp on relation_tuple_transaction(timestamp);
`

func init() {
	if err := DatabaseMigrations.Register("add-transaction-timestamp-index", "add-unique-living-ns", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, createIndexOnTupleTransactionTimestamp)
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
