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

// StaticValueOrAddColumnForSelect adds a column to the list of columns to select if the value
// is not static, otherwise it sets the value to the static value.
func StaticValueOrAddColumnForSelect(colsToSelect []any, queryInfo QueryInfo, colName string, field *string) []any {
	// If the value is static, set the field to it and return.
	if found, ok := queryInfo.FilteringValues[colName]; ok && found.SingleValue != nil {
		*field = *found.SingleValue
		return colsToSelect
	}

	// Otherwise, add the column to the list of columns to select, as the value is not static.
	colsToSelect = append(colsToSelect, field)
	return colsToSelect
}

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

// QueryRelationships queries relationships for the given query and transaction.
func QueryRelationships[R Rows, C ~map[string]any](ctx context.Context, queryInfo QueryInfo, sqlStatement string, args []any, span trace.Span, tx Querier[R], withIntegrity bool) (datastore.RelationshipIterator, error) {
	defer span.End()

	colsToSelect := make([]any, 0, 8)
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

	colsToSelect = StaticValueOrAddColumnForSelect(colsToSelect, queryInfo, queryInfo.Schema.ColNamespace, &resourceObjectType)
	colsToSelect = StaticValueOrAddColumnForSelect(colsToSelect, queryInfo, queryInfo.Schema.ColObjectID, &resourceObjectID)
	colsToSelect = StaticValueOrAddColumnForSelect(colsToSelect, queryInfo, queryInfo.Schema.ColRelation, &resourceRelation)
	colsToSelect = StaticValueOrAddColumnForSelect(colsToSelect, queryInfo, queryInfo.Schema.ColUsersetNamespace, &subjectObjectType)
	colsToSelect = StaticValueOrAddColumnForSelect(colsToSelect, queryInfo, queryInfo.Schema.ColUsersetObjectID, &subjectObjectID)
	colsToSelect = StaticValueOrAddColumnForSelect(colsToSelect, queryInfo, queryInfo.Schema.ColUsersetRelation, &subjectRelation)

	if !queryInfo.SkipCaveats {
		colsToSelect = append(colsToSelect, &caveatName, &caveatCtx)
	}

	colsToSelect = append(colsToSelect, &expiration)

	if withIntegrity {
		colsToSelect = append(colsToSelect, &integrityKeyID, &integrityHash, &timestamp)
	}

	if len(colsToSelect) == 0 {
		var unused int
		colsToSelect = append(colsToSelect, &unused)
	}

	return func(yield func(tuple.Relationship, error) bool) {
		err := tx.QueryFunc(ctx, func(ctx context.Context, rows R) error {
			var r Rows = rows
			if crwe, ok := r.(closeRowsWithError); ok {
				defer LogOnError(ctx, crwe.Close)
			} else if cr, ok := r.(closeRows); ok {
				defer cr.Close()
			}

			span.AddEvent("Query issued to database")
			relCount := 0
			for rows.Next() {
				if err := rows.Scan(colsToSelect...); err != nil {
					return fmt.Errorf(errUnableToQueryRels, fmt.Errorf("scan err: %w", err))
				}

				var caveat *corev1.ContextualizedCaveat
				if !queryInfo.SkipCaveats {
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

			if err := rows.Err(); err != nil {
				return fmt.Errorf(errUnableToQueryRels, fmt.Errorf("rows err: %w", err))
			}

			span.AddEvent("Rels loaded", trace.WithAttributes(attribute.Int("relCount", relCount)))
			return nil
		}, sqlStatement, args...)
		if err != nil {
			if !yield(tuple.Relationship{}, err) {
				return
			}
		}
	}, nil
}
