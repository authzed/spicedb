package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// See https://www.postgresql.org/docs/current/indexes-index-only-scans.html
const createRelationTupleDeletedCoveringIndex = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS ix_relation_tuple_by_deleted_xid
	ON relation_tuple (deleted_xid)
	WHERE (deleted_xid::text::bigint < 9223372036854775807);`

func init() {
	if err := DatabaseMigrations.Register("add-gc-covering-index", "drop-bigserial-ids",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, createRelationTupleDeletedCoveringIndex); err != nil {
				return err
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
