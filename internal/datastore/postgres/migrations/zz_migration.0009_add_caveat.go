package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const insertCaveatColumn = `ALTER TABLE relation_tuple ADD COLUMN caveat BYTEA;`

func init() {
	if err := DatabaseMigrations.Register("add-caveat", "add-ns-config-id", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertCaveatColumn); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
