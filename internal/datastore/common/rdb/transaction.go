package rdb

import "context"

type TransactionBeginner interface {
	BeginTransaction(ctx context.Context, readOnly bool) (Transaction, error)
}

type Transaction interface {
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)
	Exec(ctx context.Context, stmt string, args ...interface{}) error
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(...interface{}) error
}
