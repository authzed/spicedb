package common

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/pkg/datastore"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const errUnableToQueryRels = "unable to query relationships: %w"

// Querier is an interface for querying the database.
type Querier[R Rows] interface {
	QueryFunc(ctx context.Context, f func(context.Context, R) error, sql string, args ...any) error
}

// Rows is a common interface for database rows reading.
type Rows interface {
	Scan(dest ...any) error
	Next() bool
	Err() error
}

type closeRowsWithError interface {
	Rows
	Close() error
}

type closeRows interface {
	Rows
	Close()
}

func runExplainIfNecessary[R Rows](ctx context.Context, builder RelationshipsQueryBuilder, tx Querier[R], explainable datastore.Explainable) error {
	if builder.SQLExplainCallbackForTest == nil {
		return nil
	}

	// Determine the expected index names via the schema.
	expectedIndexes := builder.Schema.expectedIndexesForShape(builder.queryShape)

	// Run any pre-explain statements.
	for _, statement := range explainable.PreExplainStatements() {
		if err := tx.QueryFunc(ctx, func(ctx context.Context, rows R) error {
			rows.Next()
			return nil
		}, statement); err != nil {
			return fmt.Errorf(errUnableToQueryRels, err)
		}
	}

	// Run the query with EXPLAIN ANALYZE.
	sqlString, args, err := builder.SelectSQL()
	if err != nil {
		return fmt.Errorf(errUnableToQueryRels, err)
	}

	explainSQL, explainArgs, err := explainable.BuildExplainQuery(sqlString, args)
	if err != nil {
		return fmt.Errorf(errUnableToQueryRels, err)
	}

	err = tx.QueryFunc(ctx, func(ctx context.Context, rows R) error {
		explainString := ""
		for rows.Next() {
			var explain string
			if err := rows.Scan(&explain); err != nil {
				return fmt.Errorf(errUnableToQueryRels, fmt.Errorf("scan err: %w", err))
			}
			explainString += explain + "\n"
		}
		if explainString == "" {
			return fmt.Errorf("received empty explain")
		}

		return builder.SQLExplainCallbackForTest(ctx, sqlString, args, builder.queryShape, explainString, expectedIndexes)
	}, explainSQL, explainArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToQueryRels, err)
	}

	return nil
}

// QueryRelationships queries relationships for the given query and transaction.
func QueryRelationships[R Rows, C ~map[string]any](ctx context.Context, builder RelationshipsQueryBuilder, tx Querier[R], explainable datastore.Explainable) (datastore.RelationshipIterator, error) {
	span := trace.SpanFromContext(ctx)
	sqlString, args, err := builder.SelectSQL()
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryRels, err)
	}

	if err := runExplainIfNecessary(ctx, builder, tx, explainable); err != nil {
		return nil, err
	}

	var resourceObjectType string
	var resourceObjectID string
	var resourceRelation string
	var subjectObjectType string
	var subjectObjectID string
	var subjectRelation string
	var caveatName sql.NullString
	var caveatCtx C
	var expiration *time.Time

	var integrityKeyID string
	var integrityHash []byte
	var timestamp time.Time

	span.AddEvent("Selecting columns")
	colsToSelect, err := ColumnsToSelect(builder, &resourceObjectType, &resourceObjectID, &resourceRelation, &subjectObjectType, &subjectObjectID, &subjectRelation, &caveatName, &caveatCtx, &expiration, &integrityKeyID, &integrityHash, &timestamp)
	if err != nil {
		return nil, fmt.Errorf(errUnableToQueryRels, err)
	}

	span.AddEvent("Returning iterator", trace.WithAttributes(attribute.Int("column-count", len(colsToSelect))))
	return func(yield func(tuple.Relationship, error) bool) {
		span.AddEvent("Issuing query to database")
		err := tx.QueryFunc(ctx, func(ctx context.Context, rows R) error {
			span.AddEvent("Query issued to database")

			var r Rows = rows
			if crwe, ok := r.(closeRowsWithError); ok {
				defer LogOnError(ctx, crwe.Close)
			} else if cr, ok := r.(closeRows); ok {
				defer cr.Close()
			}

			relCount := 0
			for rows.Next() {
				if relCount == 0 {
					span.AddEvent("First row returned")
				}

				if err := rows.Scan(colsToSelect...); err != nil {
					return fmt.Errorf(errUnableToQueryRels, fmt.Errorf("scan err: %w", err))
				}

				if relCount == 0 {
					span.AddEvent("First row scanned")
				}

				var caveat *corev1.ContextualizedCaveat
				if !builder.SkipCaveats || builder.Schema.ColumnOptimization == ColumnOptimizationOptionNone {
					if caveatName.Valid {
						var err error
						caveat, err = ContextualizedCaveatFrom(caveatName.String, caveatCtx)
						if err != nil {
							return fmt.Errorf(errUnableToQueryRels, fmt.Errorf("unable to fetch caveat context: %w", err))
						}
					}
				}

				var integrity *corev1.RelationshipIntegrity
				if integrityKeyID != "" {
					integrity = &corev1.RelationshipIntegrity{
						KeyId:    integrityKeyID,
						Hash:     integrityHash,
						HashedAt: timestamppb.New(timestamp),
					}
				}

				if expiration != nil {
					// Ensure the expiration is always read in UTC, since some datastores (like CRDB)
					// will normalize to local time.
					t := expiration.UTC()
					expiration = &t
				}

				relCount++
				if !yield(tuple.Relationship{
					RelationshipReference: tuple.RelationshipReference{
						Resource: tuple.ObjectAndRelation{
							ObjectType: resourceObjectType,
							ObjectID:   resourceObjectID,
							Relation:   resourceRelation,
						},
						Subject: tuple.ObjectAndRelation{
							ObjectType: subjectObjectType,
							ObjectID:   subjectObjectID,
							Relation:   subjectRelation,
						},
					},
					OptionalCaveat:     caveat,
					OptionalExpiration: expiration,
					OptionalIntegrity:  integrity,
				}, nil) {
					return nil
				}
			}

			span.AddEvent("Relationships loaded", trace.WithAttributes(attribute.Int("relCount", relCount)))
			if err := rows.Err(); err != nil {
				return fmt.Errorf(errUnableToQueryRels, fmt.Errorf("rows err: %w", err))
			}

			return nil
		}, sqlString, args...)
		if err != nil {
			if !yield(tuple.Relationship{}, err) {
				return
			}
		}
	}, nil
}
