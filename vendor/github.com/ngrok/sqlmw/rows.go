package sqlmw

import (
	"context"
	"database/sql/driver"
	"reflect"
)

//go:generate go run ./tools/rows_picker_gen.go -o rows_picker.go

// RowsUnwrapper must be used by any middleware that provides its own wrapping
// for driver.Rows. Unwrap should return the original driver.Rows the
// middleware received. You may wish to wrap the driver.Rows returned by the
// Query methods if you want to pass extra information from the Query call to
// the subsequent RowsNext and RowsClose calls.
//
// sqlmw needs to retrieve the original driver.Rows in order to determine the
// original set of optional methods supported by the driver.Rows of the
// database driver in use by the caller.
//
// If a middleware returns a custom driver.Rows, the custom implmentation
// must support all the driver.Rows optional interfaces that are supported by
// by the drivers that will be used with it. To support any arbitrary driver
// all the optional methods must be supported.
type RowsUnwrapper interface {
	Unwrap() driver.Rows
}

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

type wrappedRowsNextResultSet struct {
	rows driver.Rows
}

func (r wrappedRowsNextResultSet) HasNextResultSet() bool {
	return r.rows.(driver.RowsNextResultSet).HasNextResultSet()
}

func (r wrappedRowsNextResultSet) NextResultSet() error {
	return r.rows.(driver.RowsNextResultSet).NextResultSet()
}

type wrappedRowsColumnTypeDatabaseTypeName struct {
	rows driver.Rows
}

func (r wrappedRowsColumnTypeDatabaseTypeName) ColumnTypeDatabaseTypeName(index int) string {
	return r.rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
}

type wrappedRowsColumnTypeLength struct {
	rows driver.Rows
}

func (r wrappedRowsColumnTypeLength) ColumnTypeLength(index int) (length int64, ok bool) {
	return r.rows.(driver.RowsColumnTypeLength).ColumnTypeLength(index)
}

type wrappedRowsColumnTypeNullable struct {
	rows driver.Rows
}

func (r wrappedRowsColumnTypeNullable) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
}

type wrappedRowsColumnTypePrecisionScale struct {
	rows driver.Rows
}

func (r wrappedRowsColumnTypePrecisionScale) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return r.rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
}

type wrappedRowsColumnTypeScanType struct {
	rows driver.Rows
}

func (r wrappedRowsColumnTypeScanType) ColumnTypeScanType(index int) reflect.Type {
	return r.rows.(driver.RowsColumnTypeScanType).ColumnTypeScanType(index)
}
