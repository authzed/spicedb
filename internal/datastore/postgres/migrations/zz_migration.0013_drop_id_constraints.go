package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

var dropIDConstraints = []string{
	`ALTER TABLE relation_tuple
		DROP CONSTRAINT uq_relation_tuple_namespace,
		DROP CONSTRAINT uq_relation_tuple_living,
		ALTER COLUMN created_transaction DROP NOT NULL,
		ALTER COLUMN deleted_transaction DROP NOT NULL`,
	`ALTER TABLE namespace_config
		DROP CONSTRAINT uq_namespace_living,
		ALTER COLUMN created_transaction DROP NOT NULL,
		ALTER COLUMN deleted_transaction DROP NOT NULL`,
	`ALTER TABLE caveat
		DROP CONSTRAINT uq_caveat_v1,
		ALTER COLUMN created_transaction DROP NOT NULL,
		ALTER COLUMN deleted_transaction DROP NOT NULL`,
}

func init() {
	if err := DatabaseMigrations.Register("drop-id-constraints", "add-xid-constraints",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range dropIDConstraints {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
