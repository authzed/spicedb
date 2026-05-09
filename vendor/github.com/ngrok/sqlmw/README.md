[![GoDoc](https://godoc.org/github.com/ngrok/sqlmw?status.svg)](https://godoc.org/github.com/ngrok/sqlmw)

# sqlmw
sqlmw provides an absurdly simple API that allows a caller to wrap a `database/sql` driver
with middleware.

This provides an abstraction similar to http middleware or GRPC interceptors but for the database/sql package.
This allows a caller to implement observability like tracing and logging easily. More importantly, it also enables
powerful possible behaviors like transparently modifying arguments, results or query execution strategy. This power allows programmers to implement
functionality like automatic sharding, selective tracing, automatic caching, transparent query mirroring, retries, fail-over 
in a reuseable way, and more.

## Usage

- Define a new type and embed the `sqlmw.NullInterceptor` type.
- Add a method you want to intercept from the `sqlmw.Interceptor` interface.
- Wrap the driver with your interceptor with `sqlmw.Driver` and then install it with `sql.Register`.
- Use `sql.Open` on the new driver string that was passed to register.

Here's a complete example:

```go
func run(dsn string) {
        // install the wrapped driver
        sql.Register("postgres-mw", sqlmw.Driver(pq.Driver{}, new(sqlInterceptor)))
        db, err := sql.Open("postgres-mw", dsn)
        ...
}

type sqlInterceptor struct {
        sqlmw.NullInterceptor
}

func (in *sqlInterceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, query string, args []driver.NamedValue) (driver.Rows, error) {
        startedAt := time.Now()
        rows, err := conn.QueryContext(ctx, args)
        log.Debug("executed sql query", "duration", time.Since(startedAt), "query", query, "args", args, "err", err)
        return rows, err
}
```

You may override any subset of methods to intercept in the `Interceptor` interface (https://godoc.org/github.com/ngrok/sqlmw#Interceptor):

```go
type Interceptor interface {
    // Connection interceptors
    ConnBeginTx(context.Context, driver.ConnBeginTx, driver.TxOptions) (driver.Tx, error)
    ConnPrepareContext(context.Context, driver.ConnPrepareContext, string) (driver.Stmt, error)
    ConnPing(context.Context, driver.Pinger) error
    ConnExecContext(context.Context, driver.ExecerContext, string, []driver.NamedValue) (driver.Result, error)
    ConnQueryContext(context.Context, driver.QueryerContext, string, []driver.NamedValue) (driver.Rows, error)

    // Connector interceptors
    ConnectorConnect(context.Context, driver.Connector) (driver.Conn, error)

    // Results interceptors
    ResultLastInsertId(driver.Result) (int64, error)
    ResultRowsAffected(driver.Result) (int64, error)

    // Rows interceptors
    RowsNext(context.Context, driver.Rows, []driver.Value) error

    // Stmt interceptors
    StmtExecContext(context.Context, driver.StmtExecContext, string, []driver.NamedValue) (driver.Result, error)
    StmtQueryContext(context.Context, driver.StmtQueryContext, string, []driver.NamedValue) (driver.Rows, error)
    StmtClose(context.Context, driver.Stmt) error

    // Tx interceptors
    TxCommit(context.Context, driver.Tx) error
    TxRollback(context.Context, driver.Tx) error
}
```

Bear in mind that because you are intercepting the calls entirely, that you are responsible for passing control up to the wrapped
driver in any function that you override, like so:

```go
func (in *sqlInterceptor) ConnPing(ctx context.Context, conn driver.Pinger) error {
    return conn.Ping(ctx)
}
```

## Examples

### Logging

```go
func (in *sqlInterceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, query string, args []driver.NamedValue) (driver.Rows, error) {
    startedAt := time.Now()
    rows, err := conn.QueryContext(ctx, args)
    log.Debug("executed sql query", "duration", time.Since(startedAt), "query", query, "args", args, "err", err)
    return rows, err
}
```

### Tracing

```go
func (in *sqlInterceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, query string, args []driver.NamedValue) (driver.Rows, error) {
    span := trace.FromContext(ctx).NewSpan(ctx, "StmtQueryContext")
    span.Tags["query"] = query
    defer span.Finish()
    rows, err := conn.QueryContext(ctx, args)
    if err != nil {
            span.Error(err)
    }
    return rows, err
}
```

### Retries

```go
func (in *sqlInterceptor) StmtQueryContext(ctx context.Context, conn driver.StmtQueryContext, query string, args []driver.NamedValue) (driver.Rows, error) {
    for {
            rows, err := conn.QueryContext(ctx, args)
            if err == nil {
                    return rows, nil
            }
            if err != nil && !isIdempotent(query) {
                    return nil, err
            }
            select {
            case <-ctx.Done():
                    return nil, ctx.Err()
            case <-time.After(time.Second):
            }
    }
}
```


## Comparison with similar projects

There are a number of other packages that allow the programmer to wrap a `database/sql/driver.Driver` to add logging or tracing.

Examples of tracing packages:
  - github.com/ExpansiveWorlds/instrumentedsql
  - contrib.go.opencensus.io/integrations/ocsql
  - gopkg.in/DataDog/dd-trace-go.v1/contrib/database/sql

A few provide a much more flexible setup of arbitrary before/after hooks to facilitate custom observability.

Packages that provide before/after hooks:
  - github.com/gchaincl/sqlhooks
  - github.com/shogo82148/go-sql-proxy

None of these packages provide an interface with sufficient power. `sqlmw` passes control of executing the
sql query to the caller which allows the caller to completely disintermediate the sql calls. This is what provides
the power to implement advanced behaviors like caching, sharding, retries, etc.

## Go version support

Go versions 1.9 and forward are supported.

## Fork

This project began by forking the code in github.com/luna-duclos/instrumentedsql, which itself is a fork of github.com/ExpansiveWorlds/instrumentedsql
