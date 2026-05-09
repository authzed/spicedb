package sqlmw

import (
	"context"
	"database/sql/driver"
)

type Interceptor interface {
	// Connection interceptors
	ConnBeginTx(context.Context, driver.ConnBeginTx, driver.TxOptions) (context.Context, driver.Tx, error)
	ConnPrepareContext(context.Context, driver.ConnPrepareContext, string) (context.Context, driver.Stmt, error)
	ConnPing(context.Context, driver.Pinger) error
	ConnExecContext(context.Context, driver.ExecerContext, string, []driver.NamedValue) (driver.Result, error)
	ConnQueryContext(context.Context, driver.QueryerContext, string, []driver.NamedValue) (context.Context, driver.Rows, error)

	// Connector interceptors
	ConnectorConnect(context.Context, driver.Connector) (driver.Conn, error)

	// Results interceptors
	ResultLastInsertId(driver.Result) (int64, error)
	ResultRowsAffected(driver.Result) (int64, error)

	// Rows interceptors
	RowsNext(context.Context, driver.Rows, []driver.Value) error
	RowsClose(context.Context, driver.Rows) error

	// Stmt interceptors
	StmtExecContext(context.Context, driver.StmtExecContext, string, []driver.NamedValue) (driver.Result, error)
	StmtQueryContext(context.Context, driver.StmtQueryContext, string, []driver.NamedValue) (context.Context, driver.Rows, error)
	StmtClose(context.Context, driver.Stmt) error

	// Tx interceptors
	TxCommit(context.Context, driver.Tx) error
	TxRollback(context.Context, driver.Tx) error
}

var _ Interceptor = NullInterceptor{}

// NullInterceptor is a complete passthrough interceptor that implements every method of the Interceptor
// interface and performs no additional logic. Users should Embed it in their own interceptor so that they
// only need to define the specific functions they are interested in intercepting.
type NullInterceptor struct{}

func (NullInterceptor) ConnBeginTx(ctx context.Context, conn driver.ConnBeginTx, txOpts driver.TxOptions) (context.Context, driver.Tx, error) {
	t, err := conn.BeginTx(ctx, txOpts)
	return ctx, t, err
}

func (NullInterceptor) ConnPrepareContext(ctx context.Context, conn driver.ConnPrepareContext, query string) (context.Context, driver.Stmt, error) {
	s, err := conn.PrepareContext(ctx, query)
	return ctx, s, err
}

func (NullInterceptor) ConnPing(ctx context.Context, conn driver.Pinger) error {
	return conn.Ping(ctx)
}

func (NullInterceptor) ConnExecContext(ctx context.Context, conn driver.ExecerContext, query string, args []driver.NamedValue) (driver.Result, error) {
	return conn.ExecContext(ctx, query, args)
}

func (NullInterceptor) ConnQueryContext(ctx context.Context, conn driver.QueryerContext, query string, args []driver.NamedValue) (context.Context, driver.Rows, error) {
	r, err := conn.QueryContext(ctx, query, args)
	return ctx, r, err
}

func (NullInterceptor) ConnectorConnect(ctx context.Context, connect driver.Connector) (driver.Conn, error) {
	return connect.Connect(ctx)
}

func (NullInterceptor) ResultLastInsertId(res driver.Result) (int64, error) {
	return res.LastInsertId()
}

func (NullInterceptor) ResultRowsAffected(res driver.Result) (int64, error) {
	return res.RowsAffected()
}

func (NullInterceptor) RowsNext(ctx context.Context, rows driver.Rows, dest []driver.Value) error {
	return rows.Next(dest)
}

func (NullInterceptor) RowsClose(ctx context.Context, rows driver.Rows) error {
	return rows.Close()
}

func (NullInterceptor) StmtExecContext(ctx context.Context, stmt driver.StmtExecContext, _ string, args []driver.NamedValue) (driver.Result, error) {
	return stmt.ExecContext(ctx, args)
}

func (NullInterceptor) StmtQueryContext(ctx context.Context, stmt driver.StmtQueryContext, _ string, args []driver.NamedValue) (context.Context, driver.Rows, error) {
	r, err := stmt.QueryContext(ctx, args)
	return ctx, r, err
}

func (NullInterceptor) StmtClose(ctx context.Context, stmt driver.Stmt) error {
	return stmt.Close()
}

func (NullInterceptor) TxCommit(ctx context.Context, tx driver.Tx) error {
	return tx.Commit()
}

func (NullInterceptor) TxRollback(ctx context.Context, tx driver.Tx) error {
	return tx.Rollback()
}
