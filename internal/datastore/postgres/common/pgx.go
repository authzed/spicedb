package common

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	zerologadapter "github.com/jackc/pgx-zerolog"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	log "github.com/authzed/spicedb/internal/logging"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const errUnableToQueryTuples = "unable to query tuples: %w"

// NewPGXQueryExecutor creates an executor that uses the pgx library to make the specified queries.
func NewPGXQueryExecutor(txSource TxFactory) common.ExecuteQueryFunc {
	return func(ctx context.Context, sql string, args []any) ([]*corev1.RelationTuple, error) {
		span := trace.SpanFromContext(ctx)

		tx, txCleanup, err := txSource(ctx)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}
		defer txCleanup(ctx)
		return queryTuples(ctx, sql, args, span, tx)
	}
}

// queryTuples queries tuples for the given query and transaction.
func queryTuples(ctx context.Context, sqlStatement string, args []any, span trace.Span, tx DBReader) ([]*corev1.RelationTuple, error) {
	span.AddEvent("DB transaction established")
	rows, err := tx.Query(ctx, sqlStatement, args...)
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}
	defer rows.Close()

	span.AddEvent("Query issued to database")

	var tuples []*corev1.RelationTuple
	for rows.Next() {
		nextTuple := &corev1.RelationTuple{
			ResourceAndRelation: &corev1.ObjectAndRelation{},
			Subject:             &corev1.ObjectAndRelation{},
		}
		var caveatName sql.NullString
		var caveatCtx map[string]any
		err := rows.Scan(
			&nextTuple.ResourceAndRelation.Namespace,
			&nextTuple.ResourceAndRelation.ObjectId,
			&nextTuple.ResourceAndRelation.Relation,
			&nextTuple.Subject.Namespace,
			&nextTuple.Subject.ObjectId,
			&nextTuple.Subject.Relation,
			&caveatName,
			&caveatCtx,
		)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
		}

		nextTuple.Caveat, err = common.ContextualizedCaveatFrom(caveatName.String, caveatCtx)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch caveat context: %w", err)
		}
		tuples = append(tuples, nextTuple)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	span.AddEvent("Tuples loaded", trace.WithAttributes(attribute.Int("tupleCount", len(tuples))))
	return tuples, nil
}

// ConfigurePGXLogger sets zerolog global logger into the connection pool configuration, and maps
// info level events to debug, as they are rather verbose for SpiceDB's info level
func ConfigurePGXLogger(connConfig *pgx.ConnConfig) {
	levelMappingFn := func(logger tracelog.Logger) tracelog.LoggerFunc {
		return func(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
			if level == tracelog.LogLevelInfo {
				level = tracelog.LogLevelDebug
			}

			// do not log cancelled queries as errors
			if errArg, ok := data["err"]; ok {
				err, ok := errArg.(error)
				if ok && errors.Is(err, context.Canceled) {
					return
				}
			}
			logger.Log(ctx, level, msg, data)
		}
	}

	l := zerologadapter.NewLogger(log.Logger, zerologadapter.WithSubDictionary("pgx"))
	connConfig.Tracer = &tracelog.TraceLog{Logger: levelMappingFn(l), LogLevel: tracelog.LogLevelInfo}
}

// DBReader copies enough of the common interface between pgxpool and tx to be useful
type DBReader interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// TxFactory returns a transaction, cleanup function, and any errors that may have
// occurred when building the transaction.
type TxFactory func(context.Context) (DBReader, common.TxCleanupFunc, error)

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
func (opts PoolOptions) ConfigurePgx(pgxConfig *pgxpool.Config) {
	if opts.MaxOpenConns != nil {
		pgxConfig.MaxConns = int32(*opts.MaxOpenConns)
	}

	if opts.MinOpenConns != nil {
		// Default to keeping the pool maxed out at all times.
		pgxConfig.MinConns = pgxConfig.MaxConns
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
}
