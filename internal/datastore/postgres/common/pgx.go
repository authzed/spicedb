package common

import (
	"context"
	"errors"
	"time"

	"github.com/ccoveille/go-safecast"
	"github.com/exaring/otelpgx"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	zerologadapter "github.com/jackc/pgx-zerolog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

// NewPGXQueryRelationshipsExecutor creates an executor that uses the pgx library to make the specified queries.
func NewPGXQueryRelationshipsExecutor(querier DBFuncQuerier, explainable datastore.Explainable) common.ExecuteReadRelsQueryFunc {
	return func(ctx context.Context, builder common.RelationshipsQueryBuilder) (datastore.RelationshipIterator, error) {
		return common.QueryRelationships[pgx.Rows, map[string]any](ctx, builder, querier, explainable)
	}
}

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
		return func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
			if level == tracelog.LogLevelInfo {
				level = tracelog.LogLevelDebug
			}

			truncateLargeSQL(data)

			// log cancellation and serialization errors at debug level
			// log revision not available errors at debug level
			if errArg, ok := data["err"]; ok {
				err, ok := errArg.(error)
				if ok && (common.IsCancellationError(err) || IsSerializationError(err) || IsReplicationLagError(err)) {
					logger.Log(ctx, tracelog.LogLevelDebug, msg, data)
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
	MinOpenConns            *int
	MaxOpenConns            *int
}

// ConfigurePgx applies PoolOptions to a pgx connection pool confiugration.
func (opts PoolOptions) ConfigurePgx(pgxConfig *pgxpool.Config, includeQueryParametersInTraces bool) error {
	if opts.MaxOpenConns != nil {
		maxConns, err := safecast.ToInt32(*opts.MaxOpenConns)
		if err != nil {
			return err
		}
		pgxConfig.MaxConns = maxConns
	}

	// Default to keeping the pool maxed out at all times.
	pgxConfig.MinConns = pgxConfig.MaxConns
	if opts.MinOpenConns != nil {
		minConns, err := safecast.ToInt32(*opts.MinOpenConns)
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
	return rowsFunc(ctx, rows)
}

func (t *QuerierFuncs) QueryRowFunc(ctx context.Context, rowFunc func(ctx context.Context, row pgx.Row) error, sql string, optionsAndArgs ...any) error {
	return rowFunc(ctx, t.d.QueryRow(ctx, sql, optionsAndArgs...))
}

func QuerierFuncsFor(d Querier) DBFuncQuerier {
	return &QuerierFuncs{d: d}
}

// SleepOnErr sleeps for a short period of time after an error has occurred.
func SleepOnErr(ctx context.Context, err error, retries uint8) {
	after := retry.BackoffExponentialWithJitter(25*time.Millisecond, 0.5)(ctx, uint(retries+1)) // add one so we always wait at least a little bit
	log.Ctx(ctx).Debug().Err(err).Dur("after", after).Uint8("retry", retries+1).Msg("retrying on database error")

	select {
	case <-time.After(after):
	case <-ctx.Done():
	}
}
