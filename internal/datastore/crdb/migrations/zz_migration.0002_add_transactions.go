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
	if err := CRDBMigrations.Register("add-transactions-table", "initial", func(apd *CRDBDriver) error {
		ctx := context.Background()

		return apd.db.BeginFunc(ctx, func(tx pgx.Tx) error {
			statements := []string{
				createTransactions,
			}
			for _, stmt := range statements {
				_, err := tx.Exec(ctx, stmt)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
