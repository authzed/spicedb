package sqlmw

import (
	"context"
	"database/sql/driver"
)

type wrappedStmt struct {
	intr   Interceptor
	ctx    context.Context
	query  string
	parent driver.Stmt
	conn   wrappedConn
}

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Stmt             = wrappedStmt{}
	_ driver.StmtExecContext  = wrappedStmt{}
	_ driver.StmtQueryContext = wrappedStmt{}
	_ driver.ColumnConverter  = wrappedStmt{}
)

func (s wrappedStmt) Close() (err error) {
	return s.intr.StmtClose(s.ctx, s.parent)
}

func (s wrappedStmt) NumInput() int {
	return s.parent.NumInput()
}

func (s wrappedStmt) Exec(args []driver.Value) (res driver.Result, err error) {
	res, err = s.parent.Exec(args)
	if err != nil {
		return nil, err
	}
	return wrappedResult{intr: s.intr, ctx: s.ctx, parent: res}, nil
}

func (s wrappedStmt) Query(args []driver.Value) (rows driver.Rows, err error) {
	rows, err = s.parent.Query(args)
	if err != nil {
		return nil, err
	}
	return wrapRows(s.ctx, s.intr, rows), nil
}

func (s wrappedStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	wrappedParent := wrappedParentStmt{Stmt: s.parent}
	res, err = s.intr.StmtExecContext(ctx, wrappedParent, s.query, args)
	if err != nil {
		return nil, err
	}
	return wrappedResult{intr: s.intr, ctx: ctx, parent: res}, nil
}

func (s wrappedStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	wrappedParent := wrappedParentStmt{Stmt: s.parent}
	ctx, rows, err = s.intr.StmtQueryContext(ctx, wrappedParent, s.query, args)
	if err != nil {
		return nil, err
	}
	return wrapRows(ctx, s.intr, rows), nil
}

func (s wrappedStmt) ColumnConverter(idx int) driver.ValueConverter {
	if converter, ok := s.parent.(driver.ColumnConverter); ok {
		return converter.ColumnConverter(idx)
	}

	return driver.DefaultParameterConverter
}

type wrappedParentStmt struct {
	driver.Stmt
}

func (s wrappedParentStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (rows driver.Rows, err error) {
	if stmtQueryContext, ok := s.Stmt.(driver.StmtQueryContext); ok {
		return stmtQueryContext.QueryContext(ctx, args)
	}
	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Stmt.Query(dargs)
}

func (s wrappedParentStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (res driver.Result, err error) {
	if stmtExecContext, ok := s.Stmt.(driver.StmtExecContext); ok {
		return stmtExecContext.ExecContext(ctx, args)
	}
	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.Stmt.Exec(dargs)
}
