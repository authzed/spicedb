package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const watchAPIIndexToRelationTupleTable = `
	relation_tuple (created_xid);`

func init() {
	if err := DatabaseMigrations.Register("add-watch-api-index-to-relation-tuple-table", "add-metadata-to-transaction-table",
		func(ctx context.Context, conn *pgx.Conn) error {
			return createIndexConcurrently(ctx, conn, "ix_watch_index", watchAPIIndexToRelationTupleTable)
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
