package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const alterTimestampDefaultValue = `
	ALTER TABLE relation_tuple_transaction 
		ALTER COLUMN timestamp SET DEFAULT (now() AT TIME ZONE 'UTC');`

func init() {
	if err := DatabaseMigrations.Register("change-transaction-timestamp-default", "add-transaction-timestamp-index",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, alterTimestampDefaultValue)
			return err
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
