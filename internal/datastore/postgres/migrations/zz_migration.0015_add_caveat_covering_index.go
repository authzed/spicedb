package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

// See https://www.postgresql.org/docs/current/indexes-index-only-scans.html
const createCaveatsCoveringIndex = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_relation_tuple_caveat_covering
	ON relation_tuple(namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, caveat_name) 
	INCLUDE (caveat_context)`

func init() {
	if err := DatabaseMigrations.Register("add-caveat-covering-index", "drop-bigserial-ids",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, createCaveatsCoveringIndex); err != nil {
				return err
			}

			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
