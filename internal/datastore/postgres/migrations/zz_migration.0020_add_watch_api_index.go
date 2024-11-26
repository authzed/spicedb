package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const addWatchAPIIndexToRelationTupleTable = `CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_watch_index ON relation_tuple (created_xid);`

func init() {
	if err := DatabaseMigrations.Register("add-watch-api-index-to-relation-tuple-table", "add-metadata-to-transaction-table",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, addWatchAPIIndexToRelationTupleTable); err != nil {
				return fmt.Errorf("failed to add watch API index to relation tuple table: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
