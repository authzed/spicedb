package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

// BYTEA type has a variable length theoretically of any length, but the undocumented limit is 1GB
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
