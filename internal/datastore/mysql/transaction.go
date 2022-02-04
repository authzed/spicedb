package mysql

import (
	"context"
	"database/sql"

	"github.com/authzed/spicedb/internal/datastore/common"

	"github.com/jmoiron/sqlx"
)

// NewMysqlTransactionBeginner constructs TransactionBeginner implementation which adapts
// a mysql database connection.
func NewMysqlTransactionBeginner(db *sqlx.DB) common.TransactionBeginner {
	return &mysqlTransactionBeginner{db}
}

type mysqlTransactionBeginner struct {
	db *sqlx.DB
}

func (mtb *mysqlTransactionBeginner) BeginTransaction(ctx context.Context, readOnly bool) (common.Transaction, error) {
	tx, err := mtb.db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	if err != nil {
		return nil, err
	}
	return &mysqlTransaction{tx}, nil
}

type mysqlTransaction struct {
	tx *sqlx.Tx
}

func (mt *mysqlTransaction) Rollback(_ context.Context) error {
	return mt.tx.Rollback()
}

func (mt *mysqlTransaction) Commit(_ context.Context) error {
	return mt.tx.Commit()
}

func (mt *mysqlTransaction) Query(ctx context.Context, query string, args ...interface{}) (common.Rows, error) {
	rows, err := mt.tx.QueryContext(ctx, query, args...) // nolint
	if err != nil {
		return nil, err
	}
	return &mysqlRows{rows}, nil
}

func (mt *mysqlTransaction) Exec(ctx context.Context, query string, args ...interface{}) error {
	_, err := mt.tx.ExecContext(ctx, query, args...)
	return err
}

type mysqlRows struct {
	*sql.Rows
}

func (mr *mysqlRows) Close() {
	common.LogOnError(context.Background(), mr.Rows.Close)
}
