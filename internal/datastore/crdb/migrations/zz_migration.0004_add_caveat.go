package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

// BYTEA type has a variable length theoretically of any length
// The recommended max length is 1MB (https://www.cockroachlabs.com/docs/stable/bytes.html)
const insertCaveatColumn = `ALTER TABLE relation_tuple ADD COLUMN caveat BYTEA;`

func init() {
	if err := CRDBMigrations.Register("add-caveat", "add-metadata-and-counters", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertCaveatColumn); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
