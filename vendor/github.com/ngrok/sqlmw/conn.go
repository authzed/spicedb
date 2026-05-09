package sqlmw

import (
	"context"
	"database/sql/driver"
)

type wrappedConn struct {
	intr   Interceptor
	parent driver.Conn
}

// Compile time validation that our types implement the expected interfaces
var (
	_ driver.Conn               = wrappedConn{}
	_ driver.ConnBeginTx        = wrappedConn{}
	_ driver.ConnPrepareContext = wrappedConn{}
	_ driver.Execer             = wrappedConn{}
	_ driver.ExecerContext      = wrappedConn{}
	_ driver.Pinger             = wrappedConn{}
	_ driver.Queryer            = wrappedConn{}
	_ driver.QueryerContext     = wrappedConn{}
)

func (c wrappedConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.parent.Prepare(query)
	if err != nil {
		return nil, err
	}
	return wrappedStmt{intr: c.intr, query: query, parent: stmt, conn: c}, nil
}

func (c wrappedConn) Close() error {
	return c.parent.Close()
}

func (c wrappedConn) Begin() (driver.Tx, error) {
	tx, err := c.parent.Begin()
	if err != nil {
		return nil, err
	}
	return wrappedTx{intr: c.intr, parent: tx}, nil
}

func (c wrappedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	wrappedParent := wrappedParentConn{c.parent}
	ctx, tx, err = c.intr.ConnBeginTx(ctx, wrappedParent, opts)
	if err != nil {
		return nil, err
	}
	return wrappedTx{intr: c.intr, ctx: ctx, parent: tx}, nil
}

func (c wrappedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	wrappedParent := wrappedParentConn{c.parent}
	ctx, stmt, err = c.intr.ConnPrepareContext(ctx, wrappedParent, query)
	if err != nil {
		return nil, err
	}
	return wrappedStmt{intr: c.intr, ctx: ctx, query: query, parent: stmt, conn: c}, nil
}

func (c wrappedConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	if execer, ok := c.parent.(driver.Execer); ok {
		res, err := execer.Exec(query, args)
		if err != nil {
			return nil, err
		}

		return wrappedResult{intr: c.intr, parent: res}, nil
	}
	return nil, driver.ErrSkip
}

func (c wrappedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	wrappedParent := wrappedParentConn{c.parent}
	r, err = c.intr.ConnExecContext(ctx, wrappedParent, query, args)
	if err != nil {
		return nil, err
	}
	return wrappedResult{intr: c.intr, ctx: ctx, parent: r}, nil
}

func (c wrappedConn) Ping(ctx context.Context) (err error) {
	if pinger, ok := c.parent.(driver.Pinger); ok {
		return c.intr.ConnPing(ctx, pinger)
	}
	return nil
}

func (c wrappedConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if queryer, ok := c.parent.(driver.Queryer); ok {
		rows, err := queryer.Query(query, args)
		if err != nil {
			return nil, err
		}
		return wrapRows(context.Background(), c.intr, rows), nil //nolint
	}
	return nil, driver.ErrSkip
}

func (c wrappedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	// Quick skip path: If the wrapped connection implements neither QueryerContext nor Queryer, we have absolutely nothing to do
	_, hasQueryerContext := c.parent.(driver.QueryerContext)
	_, hasQueryer := c.parent.(driver.Queryer)
	if !hasQueryerContext && !hasQueryer {
		return nil, driver.ErrSkip
	}

	wrappedParent := wrappedParentConn{c.parent}
	ctx, rows, err = c.intr.ConnQueryContext(ctx, wrappedParent, query, args)
	if err != nil {
		return nil, err
	}

	return wrapRows(ctx, c.intr, rows), nil
}

type wrappedParentConn struct {
	driver.Conn
}

func (c wrappedParentConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	if connBeginTx, ok := c.Conn.(driver.ConnBeginTx); ok {
		return connBeginTx.BeginTx(ctx, opts)
	}
	// Fallback implementation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return c.Conn.Begin()
	}
}

func (c wrappedParentConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if connPrepareCtx, ok := c.Conn.(driver.ConnPrepareContext); ok {
		return connPrepareCtx.PrepareContext(ctx, query)
	}
	// Fallback implementation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return c.Conn.Prepare(query)
	}
}

func (c wrappedParentConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	if execContext, ok := c.Conn.(driver.ExecerContext); ok {
		return execContext.ExecContext(ctx, query, args)
	}
	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return c.Conn.(driver.Execer).Exec(query, dargs)
	}
}

func (c wrappedParentConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	if queryerContext, ok := c.Conn.(driver.QueryerContext); ok {
		return queryerContext.QueryContext(ctx, query, args)
	}
	// Fallback implementation
	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return c.Conn.(driver.Queryer).Query(query, dargs)
	}
}
