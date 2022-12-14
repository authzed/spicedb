package common

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
)

// ConnPooler is an interface to pgx.Pool methods used by postgres-based datastores
type ConnPooler interface {
	pgxtype.Querier
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error
	Close()
}

// QueryInterceptor exposes a mechanism to intercept all methods exposed in pgxtype.Querier
// This can be used as a sort of middleware layer for pgx queries
type QueryInterceptor interface {
	// InterceptExec is the method to intercept pgxtype.Querier.Exec. The implementation is responsible to invoke the
	// delegate with the provided arguments
	InterceptExec(ctx context.Context, delegate pgxtype.Querier, sql string, arguments ...interface{}) (pgconn.CommandTag, error)

	// InterceptQuery is the method to intercept pgxtype.Querier.Query. The implementation is responsible to invoke the
	// delegate with the provided arguments
	InterceptQuery(ctx context.Context, delegate pgxtype.Querier, sql string, args ...interface{}) (pgx.Rows, error)

	// InterceptQueryRow is the method to intercept pgxtype.Querier.QueryRow. The implementation is responsible to invoke the
	// delegate with the provided arguments
	InterceptQueryRow(ctx context.Context, delegate pgxtype.Querier, sql string, optionsAndArgs ...interface{}) pgx.Row
}

type querierInterceptor struct {
	delegate    pgxtype.Querier
	interceptor QueryInterceptor
}

func newQuerierInterceptor(delegate pgxtype.Querier, interceptor QueryInterceptor) pgxtype.Querier {
	if interceptor == nil {
		return delegate
	}
	return querierInterceptor{delegate: delegate, interceptor: interceptor}
}

func (q querierInterceptor) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return q.interceptor.InterceptExec(ctx, q.delegate, sql, arguments...)
}

func (q querierInterceptor) Query(ctx context.Context, sql string, arguments ...interface{}) (pgx.Rows, error) {
	return q.interceptor.InterceptQuery(ctx, q.delegate, sql, arguments...)
}

func (q querierInterceptor) QueryRow(ctx context.Context, sql string, arguments ...interface{}) pgx.Row {
	return q.interceptor.InterceptQueryRow(ctx, q.delegate, sql, arguments...)
}

func newTxInterceptor(interceptor QueryInterceptor) interceptTxFunc {
	return func(tx pgx.Tx) pgx.Tx {
		if interceptor == nil {
			return tx
		}
		return txInterceptor{delegate: tx, interceptor: interceptor}
	}
}

type txInterceptor struct {
	delegate    pgx.Tx
	interceptor QueryInterceptor
}

type interceptTxFunc func(tx pgx.Tx) pgx.Tx

func (t txInterceptor) Begin(ctx context.Context) (pgx.Tx, error) {
	return t.delegate.Begin(ctx)
}

func (t txInterceptor) BeginFunc(ctx context.Context, f func(pgx.Tx) error) (err error) {
	return t.delegate.BeginFunc(ctx, f)
}

func (t txInterceptor) Commit(ctx context.Context) error {
	return t.delegate.Commit(ctx)
}

func (t txInterceptor) Rollback(ctx context.Context) error {
	return t.delegate.Rollback(ctx)
}

func (t txInterceptor) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return t.delegate.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (t txInterceptor) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.delegate.SendBatch(ctx, b)
}

func (t txInterceptor) LargeObjects() pgx.LargeObjects {
	return t.delegate.LargeObjects()
}

func (t txInterceptor) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return t.delegate.Prepare(ctx, name, sql)
}

func (t txInterceptor) Exec(ctx context.Context, sql string, args ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return t.interceptor.InterceptExec(ctx, t.delegate, sql, args...)
}

func (t txInterceptor) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return t.interceptor.InterceptQuery(ctx, t.delegate, sql, args...)
}

func (t txInterceptor) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return t.interceptor.InterceptQueryRow(ctx, t.delegate, sql, args...)
}

func (t txInterceptor) QueryFunc(ctx context.Context, sql string, args []interface{}, scans []interface{}, f func(pgx.QueryFuncRow) error) (pgconn.CommandTag, error) {
	return t.delegate.QueryFunc(ctx, sql, args, scans, f)
}

func (t txInterceptor) Conn() *pgx.Conn {
	return t.delegate.Conn()
}

func NewInterceptorPooler(pooler ConnPooler, interceptor QueryInterceptor) ConnPooler {
	if pooler == nil {
		panic("unexpected nil ConnPooler")
	}
	if interceptor == nil {
		return pooler
	}
	return InterceptorPooler{
		delegate:            pooler,
		interceptingQuerier: newQuerierInterceptor(pooler, interceptor),
		txInterceptor:       newTxInterceptor(interceptor),
	}
}

type InterceptorPooler struct {
	delegate            ConnPooler
	interceptingQuerier pgxtype.Querier
	txInterceptor       interceptTxFunc
}

func (i InterceptorPooler) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	return i.interceptingQuerier.Exec(ctx, sql, arguments...)
}

func (i InterceptorPooler) Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error) {
	return i.interceptingQuerier.Query(ctx, sql, optionsAndArgs...)
}

func (i InterceptorPooler) QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row {
	return i.interceptingQuerier.QueryRow(ctx, sql, optionsAndArgs...)
}

func (i InterceptorPooler) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := i.delegate.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return i.txInterceptor(tx), nil
}

func (i InterceptorPooler) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx, err := i.delegate.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	return i.txInterceptor(tx), nil
}

func (i InterceptorPooler) BeginTxFunc(ctx context.Context, txOptions pgx.TxOptions, f func(pgx.Tx) error) error {
	return i.delegate.BeginTxFunc(ctx, txOptions, func(tx pgx.Tx) error {
		return f(i.txInterceptor(tx))
	})
}

func (i InterceptorPooler) Close() {
	i.delegate.Close()
}
