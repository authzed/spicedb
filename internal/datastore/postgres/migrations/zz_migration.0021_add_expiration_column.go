package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const addExpirationColumn = `
	ALTER TABLE relation_tuple
	ADD COLUMN expiration TIMESTAMPTZ DEFAULT NULL;
`

func init() {
	if err := DatabaseMigrations.Register("add-expiration-support", "add-watch-api-index-to-relation-tuple-table",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, addExpirationColumn); err != nil {
				return fmt.Errorf("failed to add expiration column to relation tuple table: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
