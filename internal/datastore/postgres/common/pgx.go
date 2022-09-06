package common

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/zerologadapter"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	errUnableToQueryTuples = "unable to query tuples: %w"
)

// NewPGXExecutor creates an executor that uses the pgx library to make the specified queries.
func NewPGXExecutor(txSource TxFactory) common.ExecuteQueryFunc {
	return func(ctx context.Context, sql string, args []any) ([]*corev1.RelationTuple, error) {
		ctx = datastore.SeparateContextWithTracing(ctx)

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
func queryTuples(ctx context.Context, sql string, args []any, span trace.Span, tx pgx.Tx) ([]*corev1.RelationTuple, error) {
	span.AddEvent("DB transaction established")
	rows, err := tx.Query(ctx, sql, args...)
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
		err := rows.Scan(
			&nextTuple.ResourceAndRelation.Namespace,
			&nextTuple.ResourceAndRelation.ObjectId,
			&nextTuple.ResourceAndRelation.Relation,
			&nextTuple.Subject.Namespace,
			&nextTuple.Subject.ObjectId,
			&nextTuple.Subject.Relation,
		)
		if err != nil {
			return nil, fmt.Errorf(errUnableToQueryTuples, err)
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
	levelMappingFn := func(logger pgx.Logger) pgx.LoggerFunc {
		return func(ctx context.Context, level pgx.LogLevel, msg string, data map[string]interface{}) {
			if level == pgx.LogLevelInfo {
				level = pgx.LogLevelDebug
			}
			logger.Log(ctx, level, msg, data)
		}
	}
	l := zerologadapter.NewLogger(log.Logger)
	connConfig.Logger = levelMappingFn(l)
}

// TxFactory returns a transaction, cleanup function, and any errors that may have
// occurred when building the transaction.
type TxFactory func(context.Context) (pgx.Tx, common.TxCleanupFunc, error)
