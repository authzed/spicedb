package crdb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"
	"github.com/jzelinskie/stringz"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	errUnableToWriteConfig         = "unable to write namespace config: %w"
	errUnableToDeleteConfig        = "unable to delete namespace config: %w"
	errUnableToWriteRelationships  = "unable to write relationships: %w"
	errUnableToDeleteRelationships = "unable to delete relationships: %w"
	errUnableToSerializeFilter     = "unable to serialize relationship filter: %w"
)

var (
	upsertNamespaceSuffix = fmt.Sprintf(
		"ON CONFLICT (%s) DO UPDATE SET %s = excluded.%s",
		colNamespace,
		colConfig,
		colConfig,
	)
	queryWriteNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
	).Suffix(upsertNamespaceSuffix)

	queryDeleteNamespace = psql.Delete(tableNamespace)
)

type crdbReadWriteTXN struct {
	*crdbReader
	tx             pgx.Tx
	relCountChange int64
}

var (
	upsertTupleSuffixWithoutIntegrity = fmt.Sprintf(
		"ON CONFLICT (%s,%s,%s,%s,%s,%s) DO UPDATE SET %s = now(), %s = excluded.%s, %s = excluded.%s, %s = excluded.%s WHERE (relation_tuple.%s <> excluded.%s OR relation_tuple.%s <> excluded.%s OR relation_tuple.%s <> excluded.%s)",
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colTimestamp,
		colCaveatContextName,
		colCaveatContextName,
		colCaveatContext,
		colCaveatContext,
		colExpiration,
		colExpiration,
		colCaveatContextName,
		colCaveatContextName,
		colCaveatContext,
		colCaveatContext,
		colExpiration,
		colExpiration,
	)

	upsertTupleSuffixWithIntegrity = fmt.Sprintf(
		"ON CONFLICT (%s,%s,%s,%s,%s,%s) DO UPDATE SET %s = now(), %s = excluded.%s, %s = excluded.%s, %s = excluded.%s, %s = excluded.%s, %s = excluded.%s WHERE (relation_tuple_with_integrity.%s <> excluded.%s OR relation_tuple_with_integrity.%s <> excluded.%s OR relation_tuple_with_integrity.%s <> excluded.%s)",
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colTimestamp,
		colCaveatContextName,
		colCaveatContextName,
		colCaveatContext,
		colCaveatContext,
		colIntegrityKeyID,
		colIntegrityKeyID,
		colIntegrityHash,
		colIntegrityHash,
		colExpiration,
		colExpiration,
		colCaveatContextName,
		colCaveatContextName,
		colCaveatContext,
		colCaveatContext,
		colExpiration,
		colExpiration,
	)

	queryTouchTransaction = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES ($1::text) ON CONFLICT (%s) DO UPDATE SET %s = now()",
		tableTransactions,
		colTransactionKey,
		colTransactionKey,
		colTimestamp,
	)

	queryWriteCounter = psql.Insert(tableRelationshipCounter).Columns(
		colCounterName,
		colCounterSerializedFilter,
		colCounterCurrentCount,
		colCounterUpdatedAt,
	)

	queryUpdateCounter = psql.Update(tableRelationshipCounter)

	queryDeleteCounter = psql.Delete(tableRelationshipCounter)
)

func (rwt *crdbReadWriteTXN) insertQuery() sq.InsertBuilder {
	return psql.Insert(rwt.schema.RelationshipTableName)
}

func (rwt *crdbReadWriteTXN) queryDeleteTuples() sq.DeleteBuilder {
	return psql.Delete(rwt.schema.RelationshipTableName)
}

func (rwt *crdbReadWriteTXN) queryWriteTuple() sq.InsertBuilder {
	if rwt.withIntegrity {
		return rwt.insertQuery().Columns(
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
			colCaveatContextName,
			colCaveatContext,
			colExpiration,
			colIntegrityKeyID,
			colIntegrityHash,
		)
	}

	return rwt.insertQuery().Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
		colExpiration,
	)
}

func (rwt *crdbReadWriteTXN) queryTouchTuple() sq.InsertBuilder {
	if rwt.withIntegrity {
		return rwt.queryWriteTuple().Suffix(upsertTupleSuffixWithIntegrity)
	}

	return rwt.queryWriteTuple().Suffix(upsertTupleSuffixWithoutIntegrity)
}

func (rwt *crdbReadWriteTXN) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
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

	sql, args, err := queryWriteCounter.Values(
		name,
		serialized,
		0,
		nil,
	).ToSql()
	if err != nil {
		return fmt.Errorf("unable to create counter SQL: %w", err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		// If this is a constraint violation, return that the filter is already registered.
		if pgxcommon.IsConstraintFailureError(err) {
			return datastore.NewCounterAlreadyRegisteredErr(name, filter)
		}

		return fmt.Errorf("unable to register counter: %w", err)
	}

	return nil
}

func (rwt *crdbReadWriteTXN) UnregisterCounter(ctx context.Context, name string) error {
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	// Remove the counter from the table.
	sql, args, err := queryDeleteCounter.Where(sq.Eq{colCounterName: name}).ToSql()
	if err != nil {
		return fmt.Errorf("unable to unregister counter: %w", err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("unable to unregister counter: %w", err)
	}

	return nil
}

func (rwt *crdbReadWriteTXN) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	computedAtRevisionTimestamp, err := computedAtRevision.(revisions.HLCRevision).AsDecimal()
	if err != nil {
		return fmt.Errorf("unable to store counter value: %w", err)
	}

	// Update the counter in the table.
	sql, args, err := queryUpdateCounter.
		Set(colCounterCurrentCount, value).
		Set(colCounterUpdatedAt, computedAtRevisionTimestamp).
		Where(sq.Eq{colCounterName: name}).
		ToSql()
	if err != nil {
		return fmt.Errorf("unable to store counter value: %w", err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("unable to store counter value: %w", err)
	}

	return nil
}

func (rwt *crdbReadWriteTXN) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	bulkWrite := rwt.queryWriteTuple()
	var bulkWriteCount int64

	bulkTouch := rwt.queryTouchTuple()
	var bulkTouchCount int64

	bulkDelete := rwt.queryDeleteTuples()
	bulkDeleteOr := sq.Or{}
	var bulkDeleteCount int64

	// Process the actual updates
	for _, mutation := range mutations {
		rel := mutation.Relationship

		var caveatContext map[string]any
		var caveatName string
		if rel.OptionalCaveat != nil {
			caveatName = rel.OptionalCaveat.CaveatName
			caveatContext = rel.OptionalCaveat.Context.AsMap()
		}

		var integrityKeyID *string
		var integrityHash []byte

		if rel.OptionalIntegrity != nil {
			if !rwt.withIntegrity {
				return spiceerrors.MustBugf("attempted to write a relationship with integrity, but the datastore does not support integrity")
			}

			integrityKeyID = &rel.OptionalIntegrity.KeyId
			integrityHash = rel.OptionalIntegrity.Hash
		} else if rwt.withIntegrity {
			return spiceerrors.MustBugf("attempted to write a relationship without integrity, but the datastore requires integrity")
		}

		values := []any{
			rel.Resource.ObjectType,
			rel.Resource.ObjectID,
			rel.Resource.Relation,
			rel.Subject.ObjectType,
			rel.Subject.ObjectID,
			rel.Subject.Relation,
			caveatName,
			caveatContext,
			rel.OptionalExpiration,
		}

		if rwt.withIntegrity {
			values = append(values, integrityKeyID, integrityHash)
		}

		rwt.addOverlapKey(rel.Resource.ObjectType)
		rwt.addOverlapKey(rel.Subject.ObjectType)

		switch mutation.Operation {
		case tuple.UpdateOperationTouch:
			rwt.relCountChange++
			bulkTouch = bulkTouch.Values(values...)
			bulkTouchCount++

		case tuple.UpdateOperationCreate:
			rwt.relCountChange++
			bulkWrite = bulkWrite.Values(values...)
			bulkWriteCount++

		case tuple.UpdateOperationDelete:
			rwt.relCountChange--
			bulkDeleteOr = append(bulkDeleteOr, exactRelationshipClause(rel))
			bulkDeleteCount++

		default:
			log.Ctx(ctx).Error().Msg("unknown operation type")
			return fmt.Errorf("unknown mutation operation: %v", mutation.Operation)
		}
	}

	if bulkDeleteCount > 0 {
		bulkDelete = bulkDelete.Where(bulkDeleteOr)
		sql, args, err := bulkDelete.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	bulkUpdateQueries := make([]sq.InsertBuilder, 0, 2)
	if bulkWriteCount > 0 {
		bulkUpdateQueries = append(bulkUpdateQueries, bulkWrite)
	}
	if bulkTouchCount > 0 {
		bulkUpdateQueries = append(bulkUpdateQueries, bulkTouch)
	}

	for _, updateQuery := range bulkUpdateQueries {
		sql, args, err := updateQuery.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func exactRelationshipClause(r tuple.Relationship) sq.Eq {
	return sq.Eq{
		colNamespace:        r.Resource.ObjectType,
		colObjectID:         r.Resource.ObjectID,
		colRelation:         r.Resource.Relation,
		colUsersetNamespace: r.Subject.ObjectType,
		colUsersetObjectID:  r.Subject.ObjectID,
		colUsersetRelation:  r.Subject.Relation,
	}
}

func (rwt *crdbReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (bool, error) {
	// Add clauses for the ResourceFilter
	query := rwt.queryDeleteTuples()

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
			return false, fmt.Errorf("unable to delete relationships with a prefix containing the %% character")
		}

		query = query.Where(sq.Like{colObjectID: filter.OptionalResourceIdPrefix + "%"})
	}

	rwt.addOverlapKey(filter.ResourceType)

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
		rwt.addOverlapKey(subjectFilter.SubjectType)
	}

	// Add the limit, if any.
	delOpts := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	var delLimit uint64
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		delLimit = *delOpts.DeleteLimit
	}

	if delLimit > 0 {
		query = query.Limit(delLimit)
	}

	sql, args, err := query.ToSql()
	if err != nil {
		return false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	modified, err := rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	rwt.relCountChange -= modified.RowsAffected()
	rowsAffected, err := safecast.ToUint64(modified.RowsAffected())
	if err != nil {
		return false, spiceerrors.MustBugf("could not cast RowsAffected to uint64: %v", err)
	}
	if delLimit > 0 && rowsAffected == delLimit {
		return true, nil
	}

	return false, nil
}

func (rwt *crdbReadWriteTXN) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	query := queryWriteNamespace

	for _, newConfig := range newConfigs {
		rwt.addOverlapKey(newConfig.Name)

		serialized, err := newConfig.MarshalVT()
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}
		query = query.Values(newConfig.Name, serialized)
	}

	writeSQL, writeArgs, err := query.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	if _, err := rwt.tx.Exec(ctx, writeSQL, writeArgs...); err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	return nil
}

func (rwt *crdbReadWriteTXN) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	querier := pgxcommon.QuerierFuncsFor(rwt.tx)
	// For each namespace, check they exist and collect predicates for the
	// "WHERE" clause to delete the namespaces and associated tuples.
	nsClauses := make([]sq.Sqlizer, 0, len(nsNames))
	tplClauses := make([]sq.Sqlizer, 0, len(nsNames))
	for _, nsName := range nsNames {
		_, timestamp, err := rwt.loadNamespace(ctx, querier, nsName)
		if err != nil {
			if errors.As(err, &datastore.NamespaceNotFoundError{}) {
				return err
			}
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}

		for _, nsName := range nsNames {
			nsClauses = append(nsClauses, sq.Eq{colNamespace: nsName, colTimestamp: timestamp})
			tplClauses = append(tplClauses, sq.Eq{colNamespace: nsName})
		}
	}

	delSQL, delArgs, err := queryDeleteNamespace.Where(sq.Or(nsClauses)).ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := rwt.queryDeleteTuples().Where(sq.Or(tplClauses)).ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	modified, err := rwt.tx.Exec(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	numRowsDeleted := modified.RowsAffected()
	rwt.relCountChange -= numRowsDeleted

	return nil
}

var copyCols = []string{
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatContextName,
	colCaveatContext,
	colExpiration,
}

var copyColsWithIntegrity = []string{
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatContextName,
	colCaveatContext,
	colExpiration,
	colIntegrityKeyID,
	colIntegrityHash,
	colTimestamp,
}

func (rwt *crdbReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	if rwt.withIntegrity {
		return pgxcommon.BulkLoad(ctx, rwt.tx, rwt.schema.RelationshipTableName, copyColsWithIntegrity, iter)
	}

	return pgxcommon.BulkLoad(ctx, rwt.tx, rwt.schema.RelationshipTableName, copyCols, iter)
}

var _ datastore.ReadWriteTransaction = &crdbReadWriteTXN{}
