package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	createTransactions = `CREATE TABLE transactions (
    key VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL
);`
)

func init() {
	if err := CRDBMigrations.Register("add-transactions-table", "initial", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, createTransactions)
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
