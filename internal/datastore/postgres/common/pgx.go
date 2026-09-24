package common

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/exaring/otelpgx"
	zerologadapter "github.com/jackc/pgx-zerolog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/sharederrors"
	"github.com/authzed/spicedb/pkg/datastore"
)

// NewPGXQueryRelationshipsExecutor creates an executor that uses the pgx library to make the specified queries.
func NewPGXQueryRelationshipsExecutor(querier DBFuncQuerier, explainable datastore.Explainable) common.ExecuteReadRelsQueryFunc {
	return func(ctx context.Context, builder common.RelationshipsQueryBuilder) (datastore.RelationshipIterator, error) {
		return common.QueryRelationships[pgx.Rows, map[string]any](ctx, builder, querier, explainable)
	}
}

// defaultPingTimeout bounds the liveness Ping that pgxpool performs when handing
// out a connection that has been idle (see pgxpool.Config.PingTimeout, applied in
// (*Pool).Acquire). This is the only place pgxpool pings a connection for
// liveness; if it is left unset, the Ping inherits the acquiring caller's context
// verbatim, and when that context has had its deadline stripped — as happens for
// the optimized-revision computation, which runs under singleflight with
// context.WithoutCancel — a Ping against a half-open connection (e.g. one silently
// dropped by a load balancer after an idle period) blocks forever. Because every
// API request needs an optimized revision and all of them de-duplicate onto that
// one call, the entire server wedges.
//
// Setting PingTimeout bounds that Ping entirely in-process at the pgx layer: on
// timeout pgxpool destroys the dead connection and retries acquisition on the
// next one, with no global or kernel-level socket configuration involved.
const defaultPingTimeout = 5 * time.Second

// ParseConfigWithInstrumentation returns a pgx.ConnConfig that has been instrumented for observability
func ParseConfigWithInstrumentation(url string) (*pgx.ConnConfig, error) {
	connConfig, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	ConfigurePGXLogger(connConfig)
	ConfigureOTELTracer(connConfig, false)

	return connConfig, nil
}

// ConnectWithInstrumentationAndTimeout returns a pgx.Conn that has been instrumented for observability
func ConnectWithInstrumentationAndTimeout(ctx context.Context, url string, connectTimeout time.Duration) (*pgx.Conn, error) {
	connConfig, err := ParseConfigWithInstrumentation(url)
	if err != nil {
		return nil, err
	}

	connConfig.ConnectTimeout = connectTimeout
	return pgx.ConnectConfig(ctx, connConfig)
}

// ConfigurePGXLogger sets zerolog global logger into the connection pool configuration, and maps
// info level events to debug, as they are rather verbose for SpiceDB's info level
func ConfigurePGXLogger(connConfig *pgx.ConnConfig) {
	levelMappingFn := func(logger tracelog.Logger) tracelog.LoggerFunc {
		return func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
			if level == tracelog.LogLevelInfo {
				level = tracelog.LogLevelDebug
			}

			truncateLargeSQL(data)

			// log cancellation and serialization errors at debug level
			// log revision not available errors at debug level
			// expected logs don't get logged at all
			if errArg, ok := data["err"]; ok {
				err, ok := errArg.(error)
				if ok && (common.IsCancellationError(err) || IsQueryCanceledError(err) || IsSerializationError(err) || IsReplicationLagError(err)) {
					logger.Log(ctx, tracelog.LogLevelDebug, msg, data)
					return
				}

				// NOTE: this error is raised *on purpose* by the CRDB datastore when checking if watch
				// is enabled. It is not a real error, and therefore should not be logged.
				if strings.Contains(err.Error(), "negative durations are not accepted") {
					return
				}
			}

			logger.Log(ctx, level, msg, data)
		}
	}

	l := zerologadapter.NewLogger(log.Logger, zerologadapter.WithoutPGXModule(), zerologadapter.WithSubDictionary("pgx"),
		zerologadapter.WithContextFunc(func(ctx context.Context, z zerolog.Context) zerolog.Context {
			if logger := log.Ctx(ctx); logger != nil {
				return logger.With()
			}

			return z
		}))
	addTracer(connConfig, &tracelog.TraceLog{Logger: levelMappingFn(l), LogLevel: tracelog.LogLevelInfo})
}

// truncateLargeSQL takes arguments of a SQL statement provided via pgx's tracelog.LoggerFunc and
// replaces SQL statements and SQL arguments with placeholders when the statements and/or arguments
// exceed a certain length. This helps de-clutter logs when statements have hundreds to thousands of placeholders.
// The change is done in place.
func truncateLargeSQL(data map[string]any) {
	const (
		maxSQLLen     = 350
		maxSQLArgsLen = 50
	)

	if sqlData, ok := data["sql"]; ok {
		sqlString, ok := sqlData.(string)
		if ok && len(sqlString) > maxSQLLen {
			data["sql"] = sqlString[:maxSQLLen] + "..."
		}
	}
	if argsData, ok := data["args"]; ok {
		argsSlice, ok := argsData.([]any)
		if ok && len(argsSlice) > maxSQLArgsLen {
			data["args"] = argsSlice[:maxSQLArgsLen]
		}
	}
}

func IsSerializationError(err error) bool {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) &&
		// We need to check unique constraint here because some versions of postgres have an error where
		// unique constraint violations are raised instead of serialization errors.
		// (e.g. https://www.postgresql.org/message-id/flat/CAGPCyEZG76zjv7S31v_xPeLNRuzj-m%3DY2GOY7PEzu7vhB%3DyQog%40mail.gmail.com)
		(pgerr.SQLState() == pgSerializationFailure || pgerr.SQLState() == pgUniqueConstraintViolation || pgerr.SQLState() == pgTransactionAborted) {
		return true
	}

	if errors.Is(err, pgx.ErrTxCommitRollback) {
		return true
	}

	return false
}

// ConfigureOTELTracer adds OTEL tracing to a pgx.ConnConfig
func ConfigureOTELTracer(connConfig *pgx.ConnConfig, includeQueryParameters bool) {
	// otelpgx trims the SQL statement down to the operation in the span name by
	// default, which is what we want; only the extras have to be opted into.
	var options []otelpgx.Option

	if includeQueryParameters {
		options = append(options, otelpgx.WithIncludeQueryParameters())
	}

	addTracer(connConfig, otelpgx.NewTracer(options...))
}

func addTracer(connConfig *pgx.ConnConfig, tracer pgx.QueryTracer) {
	composedTracer := addComposedTracer(connConfig)
	composedTracer.Tracers = append(composedTracer.Tracers, tracer)
}

func addComposedTracer(connConfig *pgx.ConnConfig) *ComposedTracer {
	var composedTracer *ComposedTracer
	if connConfig.Tracer == nil {
		composedTracer = &ComposedTracer{}
		connConfig.Tracer = composedTracer
	} else {
		var ok bool
		composedTracer, ok = connConfig.Tracer.(*ComposedTracer)
		if !ok {
			composedTracer.Tracers = append(composedTracer.Tracers, connConfig.Tracer)
			connConfig.Tracer = composedTracer
		}
	}
	return composedTracer
}

// ComposedTracer allows adding multiple tracers to a pgx.ConnConfig
type ComposedTracer struct {
	Tracers []pgx.QueryTracer
}

func (m *ComposedTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	for _, t := range m.Tracers {
		ctx = t.TraceQueryStart(ctx, conn, data)
	}

	return ctx
}

func (m *ComposedTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	for _, t := range m.Tracers {
		t.TraceQueryEnd(ctx, conn, data)
	}
}

// DBFuncQuerier is satisfied by RetryPool and QuerierFuncs (which can wrap a pgxpool or transaction)
type DBFuncQuerier interface {
	ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error
	QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error
	QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error
}

// PoolOptions is the set of configuration used for a pgx connection pool.
type PoolOptions struct {
	ConnMaxIdleTime         *time.Duration
	ConnMaxLifetime         *time.Duration
	ConnMaxLifetimeJitter   *time.Duration
	ConnHealthCheckInterval *time.Duration
	ConnPingTimeout         *time.Duration
	MinOpenConns            *int
	MaxOpenConns            *int
}

// ConfigurePgx applies PoolOptions to a pgx connection pool confiugration.
func (opts PoolOptions) ConfigurePgx(pgxConfig *pgxpool.Config, includeQueryParametersInTraces bool) error {
	if opts.MaxOpenConns != nil {
		maxConns, err := safecast.Convert[int32](*opts.MaxOpenConns)
		if err != nil {
			return err
		}
		pgxConfig.MaxConns = maxConns
	}

	// Default to keeping the pool maxed out at all times -- but only when a
	// maximum was actually configured.
	//
	// Every path that serves real traffic configures both bounds: the engine
	// builders pass ReadConnsMinOpen/ReadConnsMaxOpen (and the write and replica
	// equivalents) from the --datastore-conn-pool-* flags on every construction,
	// so for a running SpiceDB both branches below are taken and this default is
	// invisible. It is only reached by callers that build a datastore directly
	// with no pool options at all: the test harness, and embedders using SpiceDB
	// as a library.
	//
	// For those callers, maxing out the pool means inheriting pgx's own MaxConns
	// default, max(4, NumCPU), as a *minimum* -- so on a 16-core machine every
	// datastore ever constructed insists on 16 read and 16 write connections
	// before it will hand itself back. That is a meaningful cost now that the
	// minimum is established synchronously (see WarmupPool), and it buys such a
	// caller nothing: a test needs a pool that works, not a pool that is full.
	// Keep one connection instead and let the rest fill on demand, which is what
	// pgxpool does for any pool whose minimum it has already met.
	if opts.MaxOpenConns != nil {
		pgxConfig.MinConns = pgxConfig.MaxConns
	} else {
		pgxConfig.MinConns = 1
	}

	if opts.MinOpenConns != nil {
		minConns, err := safecast.Convert[int32](*opts.MinOpenConns)
		if err != nil {
			return err
		}
		pgxConfig.MinConns = minConns
	}

	if pgxConfig.MaxConns > 0 && pgxConfig.MinConns > 0 && pgxConfig.MaxConns < pgxConfig.MinConns {
		log.Warn().Int32("max-connections", pgxConfig.MaxConns).Int32("min-connections", pgxConfig.MinConns).Msg("maximum number of connections configured is less than minimum number of connections; minimum will be used")
	}

	if opts.ConnMaxIdleTime != nil {
		pgxConfig.MaxConnIdleTime = *opts.ConnMaxIdleTime
	}

	if opts.ConnMaxLifetime != nil {
		pgxConfig.MaxConnLifetime = *opts.ConnMaxLifetime
	}

	if opts.ConnHealthCheckInterval != nil {
		pgxConfig.HealthCheckPeriod = *opts.ConnHealthCheckInterval
	}

	if opts.ConnMaxLifetimeJitter != nil {
		pgxConfig.MaxConnLifetimeJitter = *opts.ConnMaxLifetimeJitter
	} else if opts.ConnMaxLifetime != nil {
		pgxConfig.MaxConnLifetimeJitter = time.Duration(0.2 * float64(*opts.ConnMaxLifetime))
	}

	// Bound the liveness Ping pgxpool issues on acquire so a half-open connection
	// cannot hang the acquiring caller indefinitely. See defaultPingTimeout. The
	// default is applied as a safety net even when the option is unset, so every
	// pool gets a bounded ping.
	pgxConfig.PingTimeout = defaultPingTimeout
	if opts.ConnPingTimeout != nil {
		pgxConfig.PingTimeout = *opts.ConnPingTimeout
	}

	ConfigurePGXLogger(pgxConfig.ConnConfig)
	ConfigureOTELTracer(pgxConfig.ConnConfig, includeQueryParametersInTraces)
	return nil
}

type QuerierFuncs struct {
	d Querier
}

func (t *QuerierFuncs) ExecFunc(ctx context.Context, tagFunc func(ctx context.Context, tag pgconn.CommandTag, err error) error, sql string, arguments ...any) error {
	tag, err := t.d.Exec(ctx, sql, arguments...)
	return tagFunc(ctx, tag, err)
}

func (t *QuerierFuncs) QueryFunc(ctx context.Context, rowsFunc func(ctx context.Context, rows pgx.Rows) error, sql string, optionsAndArgs ...any) error {
	rows, err := t.d.Query(ctx, sql, optionsAndArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()
	err = rowsFunc(ctx, rows)
	if err != nil {
		return err
	}
	rows.Close() // drain all results sets in case it was a multi-statementy query
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func (t *QuerierFuncs) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return rowFunc(ctx, t.d.QueryRow(ctx, sql, optionsAndArgs...))
}

func QuerierFuncsFor(d Querier) DBFuncQuerier {
	return &QuerierFuncs{d: d}
}

// ConfigureDefaultQueryExecMode parses a Postgres URI and determines if a default_query_exec_mode
// has been specified. If not, it defaults to "exec".
// SpiceDB queries have high variability of arguments and rarely benefit from using prepared statements.
// The default and recommended query exec mode is 'exec', which has shown the best performance under various
// synthetic workloads. See more in https://spicedb.dev/d/query-exec-mode.
//
// The docs for the different execution modes offered by pgx may be found
// here: https://pkg.go.dev/github.com/jackc/pgx/v5#QueryExecMode
func ConfigureDefaultQueryExecMode(config *pgx.ConnConfig) {
	if !strings.Contains(config.ConnString(), "default_query_exec_mode") {
		// the execution mode was not overridden by the user
		config.DefaultQueryExecMode = pgx.QueryExecModeExec
		return
	}

	log.Info().
		Str("details-url", sharederrors.QueryExecModeErrorLink).
		Msg("found default_query_exec_mode in DB URI; leaving as-is")
}

// PoolWarmupTimeout bounds how long WarmupPool will wait for a pool to reach
// its configured minimum number of connections. It is a budget of its own,
// deliberately not shared with the datastore's other start-up work: the
// verification queries a datastore runs before warm-up vary in cost between
// engines, and sharing one deadline would make "the pool could not be filled"
// depend on how long those queries happened to take.
const PoolWarmupTimeout = 30 * time.Second

// WarmablePool is the subset of *pgxpool.Pool that WarmupPool needs. It exists
// so that pools which wrap pgxpool (such as the CockroachDB RetryPool) can be
// warmed up through the same code.
type WarmablePool interface {
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Config() *pgxpool.Config
}

// WarmupPool blocks until the pool holds its configured minimum number of
// established connections, and returns an error if it cannot get there.
//
// pgxpool.NewWithConfig does not do this itself: its last act is to start a
// goroutine that opens MinConns connections, and it then returns to the caller
// immediately. A SpiceDB process therefore finishes building its datastore --
// and is marked Ready by Kubernetes -- while its pools are still empty. The
// first burst of real traffic to a freshly rolled pod then pays connection
// establishment (TCP, TLS, authentication, and any credentials-provider token
// fetch) inline, which shows up as a latency spike or as errors. Filling the
// pool before the constructor returns moves that cost to start-up, where it is
// invisible to clients.
//
// The target is min(MinConns, MaxConns). MinConns is allowed to exceed MaxConns
// by configuration -- ConfigurePgx only warns -- but MaxConns is the hard size
// limit of the underlying resource pool, so asking for more than that would
// block until the deadline expires and never succeed.
//
// Connections are acquired concurrently and all released once the target is
// met. In practice most of them are collected from the background goroutine
// pgxpool already started rather than opened by this function.
func WarmupPool(ctx context.Context, poolName string, pool WarmablePool) error {
	config := pool.Config()
	target := min(config.MinConns, config.MaxConns)
	if target <= 0 {
		return nil
	}

	var mu sync.Mutex
	acquired := make([]*pgxpool.Conn, 0, target)

	g, gctx := errgroup.WithContext(ctx)
	for range target {
		g.Go(func() error {
			conn, err := pool.Acquire(gctx)
			if err != nil {
				return err
			}

			mu.Lock()
			defer mu.Unlock()
			acquired = append(acquired, conn)
			return nil
		})
	}

	waitErr := g.Wait()

	// Everything acquired goes straight back to the pool: the point of the
	// exercise is that the connections exist and are idle, not that this
	// function holds them.
	for _, conn := range acquired {
		conn.Release()
	}

	if waitErr != nil {
		return fmt.Errorf(
			"%s connection pool did not reach its configured minimum of %d connections (established %d); "+
				"lower the minimum connection count for this pool, or raise the connection limit on the database: %w",
			poolName, target, len(acquired), waitErr,
		)
	}

	log.Debug().Str("pool", poolName).Int32("connections", target).Msg("connection pool warmed up")
	return nil
}
