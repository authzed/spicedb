package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const addMetadataToTransactionTable = `ALTER TABLE relation_tuple_transaction ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'`

func init() {
	if err := DatabaseMigrations.Register("add-metadata-to-transaction-table", "create-relationships-counters-table",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, addMetadataToTransactionTable); err != nil {
				return fmt.Errorf("failed to add metadata to transaction table: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
