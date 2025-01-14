package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Used for cleaning up expired relationships.
const expiredRelationshipsIndex = `
	relation_tuple (expiration)
	WHERE expiration IS NOT NULL;
`

func init() {
	if err := DatabaseMigrations.Register("add-expiration-cleanup-index", "add-expiration-support",
		func(ctx context.Context, conn *pgx.Conn) error {
			return createIndexConcurrently(ctx, conn, "ix_relation_tuple_expired", expiredRelationshipsIndex)
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
