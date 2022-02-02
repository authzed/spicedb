package common

import "context"

type TransactionWrapper interface {
	Query(ctx context.Context, sql string, args ...interface{}) (DbRows, error)
	Rollback(ctx context.Context) error
}
