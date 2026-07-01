package common

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/exaring/otelpgx"
	zerologadapter "github.com/jackc/pgx-zerolog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/rs/zerolog"

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

// ConnectWithInstrumentation returns a pgx.Conn that has been instrumented for observability
func ConnectWithInstrumentation(ctx context.Context, url string) (*pgx.Conn, error) {
	connConfig, err := ParseConfigWithInstrumentation(url)
	if err != nil {
		return nil, err
	}

	return pgx.ConnectConfig(ctx, connConfig)
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
	options := []otelpgx.Option{
		otelpgx.WithTrimSQLInSpanName(),
	}

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

// TraceBatchStart, TraceBatchQuery and TraceBatchEnd implement pgx.BatchTracer.
// Without these, pgx falls back to the QueryTracer path for statements read out
// of a batch, but never calls TraceQueryStart for them (batches are traced via
// BatchTracer). The tracelog tracer then panics reading a nil context value when
// a batched statement returns rows (e.g. SHOW COMMIT TIMESTAMP). Delegating to
// any sub-tracer that implements BatchTracer both fixes that and gives batches
// real tracing spans.
func (m *ComposedTracer) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	for _, t := range m.Tracers {
		if bt, ok := t.(pgx.BatchTracer); ok {
			ctx = bt.TraceBatchStart(ctx, conn, data)
		}
	}

	return ctx
}

func (m *ComposedTracer) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	for _, t := range m.Tracers {
		if bt, ok := t.(pgx.BatchTracer); ok {
			bt.TraceBatchQuery(ctx, conn, data)
		}
	}
}

func (m *ComposedTracer) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	for _, t := range m.Tracers {
		if bt, ok := t.(pgx.BatchTracer); ok {
			bt.TraceBatchEnd(ctx, conn, data)
		}
	}
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

	// SendBatchFunc pipelines all statements queued on the batch in a single
	// network flush, then invokes resultsFunc so the caller can read each result.
	// The BatchResults are always closed after resultsFunc returns, even on error.
	//
	// On CockroachDB this collapses what would otherwise be one client<->gateway
	// round-trip per statement into a single round-trip, which dominates the
	// latency of multi-statement writes against a distributed cluster.
	SendBatchFunc(ctx context.Context, batch *pgx.Batch, resultsFunc func(ctx context.Context, results pgx.BatchResults) error) error
}

// batchSender is the subset of pgx pools/connections/transactions that can
// pipeline a batch. pgx.Tx, *pgxpool.Conn and *pgxpool.Pool all satisfy it.
type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
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

	// Default to keeping the pool maxed out at all times.
	pgxConfig.MinConns = pgxConfig.MaxConns
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

func (t *QuerierFuncs) SendBatchFunc(ctx context.Context, batch *pgx.Batch, resultsFunc func(ctx context.Context, results pgx.BatchResults) error) error {
	sender, ok := t.d.(batchSender)
	if !ok {
		return fmt.Errorf("underlying querier of type %T does not support batching", t.d)
	}

	results := sender.SendBatch(ctx, batch)
	err := resultsFunc(ctx, results)

	// Always close the BatchResults, even on error: if resultsFunc stopped early
	// (e.g. a queued statement failed) the remaining results must still be drained
	// or the connection is left in an unusable state.
	if cerr := results.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
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
	}

	log.Info().
		Str("details-url", sharederrors.QueryExecModeErrorLink).
		Msg("found default_query_exec_mode in DB URI; leaving as-is")
}
