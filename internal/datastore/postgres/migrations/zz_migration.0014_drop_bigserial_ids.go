package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

var dropIDStmts = []string{
	`ALTER TABLE relation_tuple_transaction
		DROP COLUMN id;`,
	`ALTER TABLE namespace_config
		DROP COLUMN id,
		DROP COLUMN created_transaction,
		DROP COLUMN deleted_transaction;`,
	`ALTER TABLE relation_tuple
		DROP COLUMN id,
		DROP COLUMN created_transaction,
		DROP COLUMN deleted_transaction;`,
}

func init() {
	if err := DatabaseMigrations.Register("drop-bigserial-ids", "drop-id-constraints",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range dropIDStmts {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
