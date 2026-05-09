package sqlmw

import (
	"context"
	"database/sql/driver"
)

type wrappedTx struct {
	intr   Interceptor
	ctx    context.Context
	parent driver.Tx
}

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Tx = wrappedTx{}
)

func (t wrappedTx) Commit() (err error) {
	return t.intr.TxCommit(t.ctx, t.parent)
}

func (t wrappedTx) Rollback() (err error) {
	return t.intr.TxRollback(t.ctx, t.parent)
}
