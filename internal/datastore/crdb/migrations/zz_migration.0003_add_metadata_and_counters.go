package migrations

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	createMetadataTable = `CREATE TABLE metadata (
    unique_id VARCHAR PRIMARY KEY
);`

	createCounters = `CREATE TABLE relationship_estimate_counters (
	id BYTES PRIMARY KEY,
	count INT NOT NULL
);`

	insertUniqueID = `INSERT INTO metadata (unique_id) VALUES ($1);`
)

func init() {
	if err := CRDBMigrations.Register("add-metadata-and-counters", "add-transactions-table", noNonAtomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, createMetadataTable); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, createCounters); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, insertUniqueID, uuid.NewString()); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
