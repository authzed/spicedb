package migrations

import "context"

const (
	createTransactions = `CREATE TABLE transactions (
    key VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL
);`
)

func init() {
	if err := CRDBMigrations.Register("add_transactions_table", "initial", func(apd *CRDBDriver) error {
		ctx := context.Background()

		tx, err := apd.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		statements := []string{
			createTransactions,
		}
		for _, stmt := range statements {
			_, err := tx.Exec(ctx, stmt)
			if err != nil {
				return err
			}
		}

		return tx.Commit(ctx)
	}); err != nil {
		panic(err)
	}
}
