package migrations

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	createUniqueIDTable = `CREATE TABLE metadata (
		unique_id VARCHAR PRIMARY KEY
	);`
	insertUniqueID = `INSERT INTO metadata (unique_id) VALUES ($1);`
)

func init() {
	if err := DatabaseMigrations.Register("add-unique-datastore-id", "add-gc-index", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, createUniqueIDTable); err != nil {
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
