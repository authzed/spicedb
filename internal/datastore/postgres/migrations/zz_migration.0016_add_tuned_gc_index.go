package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const createTunedGCIndex = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS ix_gc_index
	ON relation_tuple (deleted_xid DESC)
    WHERE deleted_xid < '9223372036854775807'::xid8;`

const deleteSuboptimalGCIndex = `DROP INDEX CONCURRENTLY IF EXISTS ix_relation_tuple_by_deleted_xid`

func init() {
	if err := DatabaseMigrations.Register("add-tuned-gc-index", "add-gc-covering-index",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, createTunedGCIndex); err != nil {
				return fmt.Errorf("failed to create new tuned GC Index: %w", err)
			}
			if _, err := conn.Exec(ctx, deleteSuboptimalGCIndex); err != nil {
				return fmt.Errorf("failed to remove old GC Index: %w", err)
			}
			if _, err := conn.Exec(ctx, "ANALYZE relation_tuple"); err != nil {
				return fmt.Errorf("failed to update relation_tuple table statistics after new index: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
