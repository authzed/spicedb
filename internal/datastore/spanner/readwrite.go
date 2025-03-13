package spanner

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ccoveille/go-safecast"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type spannerReadWriteTXN struct {
	spannerReader
	spannerRWT *spanner.ReadWriteTransaction
}

const inLimit = 10_000 // https://cloud.google.com/spanner/quotas#query-limits

func (rwt spannerReadWriteTXN) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	// Ensure the counter doesn't already exist.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) > 0 {
		return datastore.NewCounterAlreadyRegisteredErr(name, filter)
	}

	// Add the counter to the table.
	serialized, err := filter.MarshalVT()
	if err != nil {
		return fmt.Errorf(errUnableToSerializeFilter, err)
	}

	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.InsertOrUpdate(
			tableRelationshipCounter,
			[]string{colCounterName, colCounterSerializedFilter, colCounterCurrentCount, colCounterUpdatedAtTimestamp},
			[]any{name, serialized, 0, nil},
		),
	}); err != nil {
		return fmt.Errorf(errUnableToWriteCounter, err)
	}

	return nil
}

func (rwt spannerReadWriteTXN) UnregisterCounter(ctx context.Context, name string) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	// Delete the counter from the table.
	key := spanner.Key{name}
	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.Delete(tableRelationshipCounter, spanner.KeySetFromKeys(key)),
	}); err != nil {
		return fmt.Errorf(errUnableToDeleteCounter, err)
	}

	return nil
}

func (rwt spannerReadWriteTXN) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	// Update the counter's count and revision in the table.
	updatedTimestampTime := computedAtRevision.(revisions.TimestampRevision).Time()

	mutation := spanner.Update(tableRelationshipCounter,
		[]string{colCounterName, colCounterCurrentCount, colCounterUpdatedAtTimestamp},
		[]any{name, value, updatedTimestampTime},
	)

	if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
		return fmt.Errorf(errUnableToUpdateCounter, err)
	}

	return nil
}

func (rwt spannerReadWriteTXN) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	var rowCountChange int64
	for _, mutation := range mutations {
		txnMut, countChange, err := spannerMutation(ctx, mutation.Operation, mutation.Relationship)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
		rowCountChange += countChange

		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{txnMut}); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func spannerMutation(
	ctx context.Context,
	operation tuple.UpdateOperation,
	rel tuple.Relationship,
) (txnMut *spanner.Mutation, countChange int64, err error) {
	switch operation {
	case tuple.UpdateOperationTouch:
		countChange = 1
		txnMut = spanner.InsertOrUpdate(tableRelationship, allRelationshipCols, upsertVals(rel))
	case tuple.UpdateOperationCreate:
		countChange = 1
		txnMut = spanner.Insert(tableRelationship, allRelationshipCols, upsertVals(rel))
	case tuple.UpdateOperationDelete:
		countChange = -1
		txnMut = spanner.Delete(tableRelationship, keyFromRelationship(rel))
	default:
		log.Ctx(ctx).Error().Msg("unknown operation type")
		err = fmt.Errorf("unknown mutation operation: %v", operation)
		return
	}

	return
}

func (rwt spannerReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	numDeleted, limitReached, err := deleteWithFilter(ctx, rwt.spannerRWT, filter, opts...)
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return numDeleted, limitReached, nil
}

func deleteWithFilter(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	delOpts := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	var delLimit uint64
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		delLimit = *delOpts.DeleteLimit
		if delLimit > inLimit {
			return 0, false, spiceerrors.MustBugf("delete limit %d exceeds maximum of %d in spanner", delLimit, inLimit)
		}
	}

	var numDeleted int64
	if delLimit > 0 {
		nu, err := deleteWithFilterAndLimit(ctx, rwt, filter, delLimit)
		if err != nil {
			return 0, false, err
		}
		numDeleted = nu
	} else {
		nu, err := deleteWithFilterAndNoLimit(ctx, rwt, filter)
		if err != nil {
			return 0, false, err
		}

		numDeleted = nu
	}

	uintNumDeleted, err := safecast.ToUint64(numDeleted)
	if err != nil {
		return 0, false, spiceerrors.MustBugf("numDeleted was negative: %v", err)
	}

	if delLimit > 0 && uintNumDeleted == delLimit {
		return uintNumDeleted, true, nil
	}

	return uintNumDeleted, false, nil
}

func deleteWithFilterAndLimit(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter, delLimit uint64) (int64, error) {
	query := queryTuplesForDelete
	filteredQuery, err := applyFilterToQuery(query, filter)
	if err != nil {
		return -1, err
	}
	query = filteredQuery
	query = query.Limit(delLimit)

	sql, args, err := query.ToSql()
	if err != nil {
		return -1, err
	}

	mutations := make([]*spanner.Mutation, 0, delLimit)

	// Load the relationships to be deleted.
	iter := rwt.Query(ctx, statementFromSQL(sql, args))
	defer iter.Stop()

	if err := iter.Do(func(row *spanner.Row) error {
		var resourceObjectType string
		var resourceObjectID string
		var relation string
		var subjectObjectType string
		var subjectObjectID string
		var subjectRelation string

		err := row.Columns(
			&resourceObjectType,
			&resourceObjectID,
			&relation,
			&subjectObjectType,
			&subjectObjectID,
			&subjectRelation,
		)
		if err != nil {
			return err
		}

		nextRel := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: resourceObjectType,
					ObjectID:   resourceObjectID,
					Relation:   relation,
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: subjectObjectType,
					ObjectID:   subjectObjectID,
					Relation:   subjectRelation,
				},
			},
		}

		mutations = append(mutations, spanner.Delete(tableRelationship, keyFromRelationship(nextRel)))
		return nil
	}); err != nil {
		return -1, err
	}

	// Delete the relationships.
	if err := rwt.BufferWrite(mutations); err != nil {
		return -1, fmt.Errorf(errUnableToWriteRelationships, err)
	}

	return int64(len(mutations)), nil
}

func deleteWithFilterAndNoLimit(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter) (int64, error) {
	query := sql.Delete(tableRelationship)
	filteredQuery, err := applyFilterToQuery(query, filter)
	if err != nil {
		return -1, err
	}
	query = filteredQuery

	sql, args, err := query.ToSql()
	if err != nil {
		return -1, err
	}

	deleteStatement := statementFromSQL(sql, args)
	return rwt.Update(ctx, deleteStatement)
}

type builder[T any] interface {
	Where(pred interface{}, args ...interface{}) T
}

func applyFilterToQuery[T builder[T]](query T, filter *v1.RelationshipFilter) (T, error) {
	// Add clauses for the ResourceFilter
	if filter.ResourceType != "" {
		query = query.Where(sq.Eq{colNamespace: filter.ResourceType})
	}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
	}
	if filter.OptionalResourceIdPrefix != "" {
		if strings.Contains(filter.OptionalResourceIdPrefix, "%") {
			return query, fmt.Errorf("unable to delete relationships with a prefix containing the %% character")
		}

		query = query.Where(sq.Like{colObjectID: filter.OptionalResourceIdPrefix + "%"})
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	return query, nil
}

func upsertVals(r tuple.Relationship) []any {
	key := keyFromRelationship(r)
	key = append(key, spanner.CommitTimestamp)
	key = append(key, caveatVals(r)...)

	if r.OptionalExpiration != nil {
		key = append(key, spanner.NullTime{Time: *r.OptionalExpiration, Valid: true})
	} else {
		key = append(key, nil)
	}
	return key
}

func keyFromRelationship(r tuple.Relationship) spanner.Key {
	return spanner.Key{
		r.Resource.ObjectType,
		r.Resource.ObjectID,
		r.Resource.Relation,
		r.Subject.ObjectType,
		r.Subject.ObjectID,
		r.Subject.Relation,
	}
}

func caveatVals(r tuple.Relationship) []any {
	if r.OptionalCaveat == nil {
		return []any{"", nil}
	}
	vals := []any{r.OptionalCaveat.CaveatName}
	if r.OptionalCaveat.Context != nil {
		vals = append(vals, spanner.NullJSON{Value: r.OptionalCaveat.Context, Valid: true})
	} else {
		vals = append(vals, nil)
	}
	return vals
}

func (rwt spannerReadWriteTXN) WriteNamespaces(_ context.Context, newConfigs ...*core.NamespaceDefinition) error {
	mutations := make([]*spanner.Mutation, 0, len(newConfigs))
	for _, newConfig := range newConfigs {
		serialized, err := newConfig.MarshalVT()
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		mutations = append(mutations, spanner.InsertOrUpdate(
			tableNamespace,
			[]string{colNamespaceName, colNamespaceConfig, colTimestamp},
			[]any{newConfig.Name, serialized, spanner.CommitTimestamp},
		))
	}

	return rwt.spannerRWT.BufferWrite(mutations)
}

func (rwt spannerReadWriteTXN) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	namespaces, err := rwt.LookupNamespacesWithNames(ctx, nsNames)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	if len(namespaces) != len(nsNames) {
		expectedNamespaceNames := mapz.NewSet[string](nsNames...)
		for _, ns := range namespaces {
			expectedNamespaceNames.Delete(ns.Definition.Name)
		}

		return fmt.Errorf(errUnableToDeleteConfig, fmt.Errorf("namespaces not found: %v", expectedNamespaceNames.AsSlice()))
	}

	for _, nsName := range nsNames {
		// Ensure the namespace exists.

		relFilter := &v1.RelationshipFilter{ResourceType: nsName}
		if _, _, err := deleteWithFilter(ctx, rwt.spannerRWT, relFilter); err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}

		err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
			spanner.Delete(tableNamespace, spanner.KeySetFromKeys(spanner.Key{nsName})),
		})
		if err != nil {
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	return nil
}

func (rwt spannerReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	var numLoaded uint64
	var rel *tuple.Relationship
	var err error
	for rel, err = iter.Next(ctx); err == nil && rel != nil; rel, err = iter.Next(ctx) {
		txnMut, _, err := spannerMutation(ctx, tuple.UpdateOperationCreate, *rel)
		if err != nil {
			return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
		}
		numLoaded++

		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{txnMut}); err != nil {
			return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
		}
	}

	if err != nil {
		return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
	}

	return numLoaded, nil
}

var _ datastore.ReadWriteTransaction = spannerReadWriteTXN{}
