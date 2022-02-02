package rdb

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func NewPostgresTransactionBeginner(pool *pgxpool.Pool) TransactionBeginner {
	return &postgresTransactionBeginner{pool}
}

type postgresTransactionBeginner struct {
	pool *pgxpool.Pool
}

func (ptb *postgresTransactionBeginner) BeginTransaction(ctx context.Context, readOnly bool) (Transaction, error) {
	var opts pgx.TxOptions
	if readOnly {
		opts.AccessMode = pgx.ReadOnly
	}
	tx, err := ptb.pool.BeginTx(ctx, opts)
	return NewPostgresTransaction(tx), err
}

func NewPostgresTransaction(tx pgx.Tx) Transaction {
	return &postgresTransaction{tx}
}

type postgresTransaction struct {
	pgx.Tx
}

// NOTE(chriskirkland): need to adapt this explicitly because the pgx.Rows interface is not the desired Rows
// interface, even though it satisfies it.
func (pt *postgresTransaction) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return pt.Tx.Query(ctx, query, args...)
}

func (pt *postgresTransaction) Exec(ctx context.Context, stmt string, args ...interface{}) error {
	_, err := pt.Tx.Exec(ctx, stmt, args...)
	return err
}
