package postgres

import (
	"context"
	"database/sql/driver"

	"github.com/ngrok/sqlmw"
)

type traceInterceptor struct {
	sqlmw.NullInterceptor
}

func (ti *traceInterceptor) ConnBeginTx(ctx context.Context, conn driver.ConnBeginTx, opts driver.TxOptions) (context.Context, driver.Tx, error) {
	ctx, span := tracer.Start(ctx, "ConnBeginTx")
	defer span.End()

	tx, err := conn.BeginTx(ctx, opts)
	return ctx, tx, err
}

func (ti *traceInterceptor) ConnPrepareContext(ctx context.Context, conn driver.ConnPrepareContext, query string) (context.Context, driver.Stmt, error) {
	ctx, span := tracer.Start(ctx, "ConnPrepareContext")
	defer span.End()
	stmt, err := conn.PrepareContext(ctx, query)
	return ctx, stmt, err
}

func (ti *traceInterceptor) ConnPing(ctx context.Context, conn driver.Pinger) error {
	ctx, span := tracer.Start(ctx, "ConnPing")
	defer span.End()

	return conn.Ping(ctx)
}

func (ti *traceInterceptor) ConnExecContext(ctx context.Context, conn driver.ExecerContext, query string, args []driver.NamedValue) (driver.Result, error) {
	ctx, span := tracer.Start(ctx, "ConnExecContext")
	defer span.End()

	return conn.ExecContext(ctx, query, args)
}

func (ti *traceInterceptor) ConnQueryContext(ctx context.Context, conn driver.QueryerContext, query string, args []driver.NamedValue) (context.Context, driver.Rows, error) {
	ctx, span := tracer.Start(ctx, "ConnQueryContext")
	defer span.End()

	rows, err := conn.QueryContext(ctx, query, args)
	return ctx, rows, err
}

// Connector interceptors
func (ti *traceInterceptor) ConnectorConnect(ctx context.Context, conn driver.Connector) (driver.Conn, error) {
	ctx, span := tracer.Start(ctx, "ConnectorConnect")
	defer span.End()

	return conn.Connect(ctx)
}

// Rows interceptors
func (ti *traceInterceptor) RowsNext(ctx context.Context, conn driver.Rows, dest []driver.Value) error {
	_, span := tracer.Start(ctx, "RowsNext")
	defer span.End()

	return conn.Next(dest)
}

// Stmt interceptors
func (ti *traceInterceptor) StmtExecContext(ctx context.Context, conn driver.StmtExecContext, _ string, args []driver.NamedValue) (driver.Result, error) {
	ctx, span := tracer.Start(ctx, "StmtExecContext")
	defer span.End()

	return conn.ExecContext(ctx, args)
}

func (ti *traceInterceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, _ string, args []driver.NamedValue) (context.Context, driver.Rows, error) {
	ctx, span := tracer.Start(ctx, "StmtQueryContext")
	defer span.End()

	rows, err := conn.QueryContext(ctx, args)
	return ctx, rows, err
}

func (ti *traceInterceptor) StmtClose(ctx context.Context, conn driver.Stmt) error {
	_, span := tracer.Start(ctx, "StmtClose")
	defer span.End()

	return conn.Close()
}

// Tx interceptors
func (ti *traceInterceptor) TxCommit(ctx context.Context, conn driver.Tx) error {
	_, span := tracer.Start(ctx, "TxCommit")
	defer span.End()

	return conn.Commit()
}

func (ti *traceInterceptor) TxRollback(ctx context.Context, conn driver.Tx) error {
	_, span := tracer.Start(ctx, "TxRollback")
	defer span.End()

	return conn.Rollback()
}
