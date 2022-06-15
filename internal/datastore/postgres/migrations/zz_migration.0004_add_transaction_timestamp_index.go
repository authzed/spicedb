package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const createIndexOnTupleTransactionTimestamp = `
	CREATE INDEX ix_relation_tuple_transaction_by_timestamp on relation_tuple_transaction(timestamp);
`

func init() {
	if err := DatabaseMigrations.Register("add-transaction-timestamp-index", "add-unique-living-ns", func(ctx context.Context, conn *pgx.Conn, version, replaced string) error {
		return commitWithMigrationVersion(ctx, conn, version, replaced, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, createIndexOnTupleTransactionTimestamp)
			return err
		})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
