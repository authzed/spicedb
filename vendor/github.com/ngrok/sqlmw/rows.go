package sqlmw

import (
	"context"
	"database/sql/driver"
)

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Rows                           = wrappedRows{}
	_ driver.RowsColumnTypeDatabaseTypeName // TODO
	_ driver.RowsColumnTypeLength           // TODO
	_ driver.RowsColumnTypeNullable         // TODO
	_ driver.RowsColumnTypePrecisionScale   // TODO
	_ driver.RowsColumnTypeScanType         // TODO
	_ driver.RowsNextResultSet              // TODO
)

type wrappedRows struct {
	intr   Interceptor
	ctx    context.Context
	parent driver.Rows
}

func (r wrappedRows) Columns() []string {
	return r.parent.Columns()
}

func (r wrappedRows) Close() error {
	return r.intr.RowsClose(r.ctx, r.parent)
}

func (r wrappedRows) Next(dest []driver.Value) (err error) {
	return r.intr.RowsNext(r.ctx, r.parent, dest)
}
