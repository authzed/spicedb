package rdb

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

//TODO(chriskirkland):
// - add an implementation for Postgres/CRDB
// - plug it into Postgres/CRDB and run tests to validate

func NewMysqlTransactionBeginner(db *sqlx.DB) TransactionBeginner {
	return &mysqlTransactionBeginner{db}
}

type mysqlTransactionBeginner struct {
	db *sqlx.DB
}

func (mtb *mysqlTransactionBeginner) BeginTransaction(ctx context.Context, readOnly bool) (Transaction, error) {
	tx, err := mtb.db.BeginTxx(ctx, &sql.TxOptions{ReadOnly: readOnly})
	return &mysqlTransaction{tx}, err
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

func (mt *mysqlTransaction) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	rows, err := mt.tx.QueryContext(ctx, query, args...)
	return &mysqlRows{rows}, err
}

type mysqlRows struct {
	*sql.Rows
}

func (mr *mysqlRows) Close() {
	// ignore the error to satify the Rows interface
	mr.Rows.Close()
}
