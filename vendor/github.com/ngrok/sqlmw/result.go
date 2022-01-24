package sqlmw

import (
	"context"
	"database/sql/driver"
)

type wrappedResult struct {
	intr   Interceptor
	ctx    context.Context
	parent driver.Result
}

func (r wrappedResult) LastInsertId() (id int64, err error) {
	return r.intr.ResultLastInsertId(r.parent)
}

func (r wrappedResult) RowsAffected() (num int64, err error) {
	return r.intr.ResultRowsAffected(r.parent)
}
