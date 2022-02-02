package rdb

import "context"

type TransactionBeginner interface {
	BeginTransaction(ctx context.Context, readOnly bool) (Transaction, error)
}

type Transaction interface {
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

type Rows interface {
	Close() error
	Columns() ([]string, error)
	Err() error
	Next() bool
	Scan(...interface{}) error
}
