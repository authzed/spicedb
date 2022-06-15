package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	createDeletedTransactionIndex = `CREATE INDEX CONCURRENTLY ix_relation_tuple_by_deleted_transaction ON relation_tuple (deleted_transaction)`
)

func init() {
	if err := DatabaseMigrations.Register("add-gc-index", "change-transaction-timestamp-default", func(ctx context.Context, conn *pgx.Conn, version, replaced string) error {
		// CREATE INDEX CONCURRENTLY cannot run inside a transaction block (SQLSTATE 25001)
		if _, err := conn.Exec(ctx, createDeletedTransactionIndex); err != nil {
			return err
		}
		return commitWithMigrationVersion(ctx, conn, version, replaced, func(tx pgx.Tx) error {
			return nil
		})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
