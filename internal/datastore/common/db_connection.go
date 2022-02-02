package common

import "context"

type DbConnection interface {
	BeginTxx(ctx context.Context, readOnly bool) (TransactionWrapper, error)
}
