package mysql

import (
	"context"
	"database/sql"

	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
)

// BeginTXFunc is a polyfill for database/sql which does a closure style transaction lifecycle.
// The underlying transaction is aborted if the supplied function returns an error.
// The underlying transaction is committed if the supplied function returns nil.
func BeginTxFunc(ctx context.Context, db *sql.DB, txOptions *sql.TxOptions, f func(*sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	defer migrations.LogOnError(ctx, tx.Rollback)

	if err := f(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}
