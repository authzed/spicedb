package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ccoveille/go-safecast"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	typedschema "github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/spiceerrors"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	errUnableToWriteConfig                = "unable to write namespace config: %w"
	errUnableToDeleteConfig               = "unable to delete namespace config: %w"
	errUnableToWriteRelationships         = "unable to write relationships: %w"
	errUnableToDeleteRelationships        = "unable to delete relationships: %w"
	errUnableToWriteRelationshipsCounter  = "unable to write relationships counter: %w"
	errUnableToDeleteRelationshipsCounter = "unable to delete relationships counter: %w"
)

var (
	writeNamespace = psql.Insert(schema.TableNamespace).Columns(
		schema.ColNamespace,
		schema.ColConfig,
	)

	deleteNamespace = psql.Update(schema.TableNamespace).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})

	deleteNamespaceTuples = psql.Update(schema.TableTuple).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})

	writeTuple = psql.Insert(schema.TableTuple).Columns(
		schema.ColNamespace,
		schema.ColObjectID,
		schema.ColRelation,
		schema.ColUsersetNamespace,
		schema.ColUsersetObjectID,
		schema.ColUsersetRelation,
		schema.ColCaveatContextName,
		schema.ColCaveatContext,
		schema.ColExpiration,
	)

	deleteTuple     = psql.Update(schema.TableTuple).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})
	selectForDelete = psql.Select(
		schema.ColNamespace,
		schema.ColObjectID,
		schema.ColRelation,
		schema.ColUsersetNamespace,
		schema.ColUsersetObjectID,
		schema.ColUsersetRelation,
		schema.ColCreatedXid,
	).From(schema.TableTuple).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})

	writeRelationshipCounter = psql.Insert(schema.TableRelationshipCounter).Columns(
		schema.ColCounterName,
		schema.ColCounterFilter,
		schema.ColCounterCurrentCount,
		schema.ColCounterSnapshot,
	)

	updateRelationshipCounter = psql.Update(schema.TableRelationshipCounter).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})

	deleteRelationshipCounter = psql.Update(schema.TableRelationshipCounter).Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})
)

type pgReadWriteTXN struct {
	*pgReader
	tx     pgx.Tx
	newXID xid8
}

func appendForInsertion(builder sq.InsertBuilder, tpl tuple.Relationship) sq.InsertBuilder {
	var caveatName string
	var caveatContext map[string]any
	if tpl.OptionalCaveat != nil {
		caveatName = tpl.OptionalCaveat.CaveatName
		caveatContext = tpl.OptionalCaveat.Context.AsMap()
	}

	valuesToWrite := []interface{}{
		tpl.Resource.ObjectType,
		tpl.Resource.ObjectID,
		tpl.Resource.Relation,
		tpl.Subject.ObjectType,
		tpl.Subject.ObjectID,
		tpl.Subject.Relation,
		caveatName,
		caveatContext, // PGX driver serializes map[string]any to JSONB columns,
		tpl.OptionalExpiration,
	}

	return builder.Values(valuesToWrite...)
}

func (rwt *pgReadWriteTXN) collectSimplifiedTouchTypes(ctx context.Context, mutations []tuple.RelationshipUpdate) (*mapz.Set[string], error) {
	// Collect the list of namespaces used for resources for relationships being TOUCHed.
	touchedResourceNamespaces := mapz.NewSet[string]()
	for _, mut := range mutations {
		if mut.Operation == tuple.UpdateOperationTouch {
			touchedResourceNamespaces.Add(mut.Relationship.Resource.ObjectType)
		}
	}

	// Load the namespaces for any resources that are being TOUCHed and check if the relation being touched
	// *can* have a caveat. If not, mark the relation as supported simplified TOUCH operations.
	relationSupportSimplifiedTouch := mapz.NewSet[string]()
	if touchedResourceNamespaces.IsEmpty() {
		return relationSupportSimplifiedTouch, nil
	}

	namespaces, err := rwt.LookupNamespacesWithNames(ctx, touchedResourceNamespaces.AsSlice())
	if err != nil {
		return nil, handleWriteError(err)
	}

	if len(namespaces) == 0 {
		return relationSupportSimplifiedTouch, nil
	}

	nsDefByName := make(map[string]*core.NamespaceDefinition, len(namespaces))
	for _, ns := range namespaces {
		nsDefByName[ns.Definition.Name] = ns.Definition
	}

	for _, mut := range mutations {
		rel := mut.Relationship
		if mut.Operation != tuple.UpdateOperationTouch {
			continue
		}

		nsDef, ok := nsDefByName[rel.Resource.ObjectType]
		if !ok {
			continue
		}

		vts, err := typedschema.NewDefinition(typedschema.NewTypeSystem(typedschema.ResolverForDatastoreReader(rwt)), nsDef)
		if err != nil {
			return nil, handleWriteError(err)
		}

		notAllowed, err := vts.RelationDoesNotAllowCaveatsOrTraitsForSubject(rel.Resource.Relation, rel.Subject.ObjectType)
		if err != nil {
			// Ignore errors and just fallback to the less efficient path.
			continue
		}

		if notAllowed {
			relationSupportSimplifiedTouch.Add(nsDef.Name + "#" + rel.Resource.Relation + "@" + rel.Subject.ObjectType)
			continue
		}
	}

	return relationSupportSimplifiedTouch, nil
}

func (rwt *pgReadWriteTXN) WriteRelationships(ctx context.Context, mutations []tuple.RelationshipUpdate) error {
	touchMutationsByNonCaveat := make(map[string]tuple.RelationshipUpdate, len(mutations))
	hasCreateInserts := false

	createInserts := writeTuple
	touchInserts := writeTuple

	deleteClauses := sq.Or{}

	// Determine the set of relation+subject types for whom a "simplified" TOUCH operation can be used. A
	// simplified TOUCH operation is one in which the relationship does not support caveats for the subject
	// type. In such cases, the "DELETE" operation is unnecessary because the relationship does not support
	// caveats for the subject type, and thus the relationship can be TOUCHed without needing to check for
	// the existence of a relationship with a different caveat name and/or context which might need to be
	// replaced.
	relationSupportSimplifiedTouch, err := rwt.collectSimplifiedTouchTypes(ctx, mutations)
	if err != nil {
		return err
	}

	// Parse the updates, building inserts for CREATE/TOUCH and deletes for DELETE.
	for _, mut := range mutations {
		rel := mut.Relationship

		switch mut.Operation {
		case tuple.UpdateOperationCreate:
			createInserts = appendForInsertion(createInserts, rel)
			hasCreateInserts = true

		case tuple.UpdateOperationTouch:
			touchInserts = appendForInsertion(touchInserts, rel)
			touchMutationsByNonCaveat[tuple.StringWithoutCaveatOrExpiration(rel)] = mut

		case tuple.UpdateOperationDelete:
			deleteClauses = append(deleteClauses, exactRelationshipClause(rel))

		default:
			return spiceerrors.MustBugf("unknown tuple mutation: %v", mut)
		}
	}

	// Run CREATE insertions, if any.
	if hasCreateInserts {
		sql, args, err := createInserts.ToSql()
		if err != nil {
			return handleWriteError(err)
		}

		_, err = rwt.tx.Exec(ctx, sql, args...)
		if err != nil {
			return handleWriteError(err)
		}
	}

	// For each of the TOUCH operations, invoke the INSERTs, but with `ON CONFLICT DO NOTHING` to ensure
	// that the operations over existing relationships no-op.
	if len(touchMutationsByNonCaveat) > 0 {
		touchInserts = touchInserts.Suffix(fmt.Sprintf("ON CONFLICT DO NOTHING RETURNING %s, %s, %s, %s, %s, %s",
			schema.ColNamespace,
			schema.ColObjectID,
			schema.ColRelation,
			schema.ColUsersetNamespace,
			schema.ColUsersetObjectID,
			schema.ColUsersetRelation,
		))

		sql, args, err := touchInserts.ToSql()
		if err != nil {
			return handleWriteError(err)
		}

		rows, err := rwt.tx.Query(ctx, sql, args...)
		if err != nil {
			return handleWriteError(err)
		}
		defer rows.Close()

		// Remove from the TOUCH map of operations each row that was successfully inserted.
		// This will cover any TOUCH that created an entirely new relationship, acting like
		// a CREATE.
		for rows.Next() {
			var resourceObjectType string
			var resourceObjectID string
			var relation string
			var subjectObjectType string
			var subjectObjectID string
			var subjectRelation string

			err := rows.Scan(
				&resourceObjectType,
				&resourceObjectID,
				&relation,
				&subjectObjectType,
				&subjectObjectID,
				&subjectRelation,
			)
			if err != nil {
				return handleWriteError(err)
			}

			rel := tuple.Relationship{
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

			tplString := tuple.StringWithoutCaveatOrExpiration(rel)
			_, ok := touchMutationsByNonCaveat[tplString]
			if !ok {
				return spiceerrors.MustBugf("missing expected completed TOUCH mutation")
			}

			delete(touchMutationsByNonCaveat, tplString)
		}
		rows.Close()

		// For each remaining TOUCH mutation, add a "DELETE" operation for the row such that if the caveat and/or
		// context has changed, the row will be deleted. For ones in which the caveat name and/or context did cause
		// the deletion (because of a change), the row will be re-inserted with the new caveat name and/or context.
		for _, mut := range touchMutationsByNonCaveat {
			// If the relation support a simplified TOUCH operation, then skip the DELETE operation, as it is unnecessary
			// because the relation does not support a caveat for a subject of this type.
			if relationSupportSimplifiedTouch.Has(mut.Relationship.Resource.ObjectType + "#" + mut.Relationship.Resource.Relation + "@" + mut.Relationship.Subject.ObjectType) {
				continue
			}

			deleteClauses = append(deleteClauses, exactRelationshipDifferentCaveatAndExpirationClause(mut.Relationship))
		}
	}

	// Execute the "DELETE" operation (an UPDATE with setting the deletion ID to the current transaction ID)
	// for any DELETE mutations or TOUCH mutations that matched existing relationships and whose caveat name
	// or context is different in some manner. We use RETURNING to determine which TOUCHed relationships were
	// deleted by virtue of their caveat name and/or context being changed.
	if len(deleteClauses) == 0 {
		// Nothing more to do.
		return nil
	}

	builder := deleteTuple.
		Where(deleteClauses).
		Suffix(fmt.Sprintf("RETURNING %s, %s, %s, %s, %s, %s",
			schema.ColNamespace,
			schema.ColObjectID,
			schema.ColRelation,
			schema.ColUsersetNamespace,
			schema.ColUsersetObjectID,
			schema.ColUsersetRelation,
		))

	sql, args, err := builder.
		Set(schema.ColDeletedXid, rwt.newXID).
		ToSql()
	if err != nil {
		return handleWriteError(err)
	}

	rows, err := rwt.tx.Query(ctx, sql, args...)
	if err != nil {
		return handleWriteError(err)
	}
	defer rows.Close()

	// For each deleted row representing a TOUCH, recreate with the new caveat and/or context.
	touchWrite := writeTuple
	touchWriteHasValues := false

	for rows.Next() {
		var resourceObjectType string
		var resourceObjectID string
		var relation string
		var subjectObjectType string
		var subjectObjectID string
		var subjectRelation string

		err := rows.Scan(
			&resourceObjectType,
			&resourceObjectID,
			&relation,
			&subjectObjectType,
			&subjectObjectID,
			&subjectRelation,
		)
		if err != nil {
			return handleWriteError(err)
		}

		deletedTpl := tuple.Relationship{
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

		tplString := tuple.StringWithoutCaveatOrExpiration(deletedTpl)
		mutation, ok := touchMutationsByNonCaveat[tplString]
		if !ok {
			// This did not represent a TOUCH operation.
			continue
		}

		touchWrite = appendForInsertion(touchWrite, mutation.Relationship)
		touchWriteHasValues = true
	}
	rows.Close()

	// If no INSERTs are necessary to update caveats, then nothing more to do.
	if !touchWriteHasValues {
		return nil
	}

	// Otherwise execute the INSERTs for the caveated-changes TOUCHed relationships.
	sql, args, err = touchWrite.ToSql()
	if err != nil {
		return handleWriteError(err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return handleWriteError(err)
	}

	return nil
}

func handleWriteError(err error) error {
	if pgxcommon.IsSerializationError(err) {
		return common.NewSerializationError(fmt.Errorf("unable to write relationships due to a serialization error: [%w]; this typically indicates that a number of write transactions are contending over the same relationships; either reduce the contention or scale this Postgres instance", err))
	}

	return fmt.Errorf(errUnableToWriteRelationships, err)
}

func (rwt *pgReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (uint64, bool, error) {
	delOpts := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		return rwt.deleteRelationshipsWithLimit(ctx, filter, *delOpts.DeleteLimit)
	}

	numDeleted, err := rwt.deleteRelationships(ctx, filter)
	return numDeleted, false, err
}

func (rwt *pgReadWriteTXN) deleteRelationshipsWithLimit(ctx context.Context, filter *v1.RelationshipFilter, limit uint64) (uint64, bool, error) {
	// validate the limit
	intLimit, err := safecast.ToInt64(limit)
	if err != nil {
		return 0, false, fmt.Errorf("limit argument could not safely be cast to int64: %w", err)
	}

	// Construct a select query for the relationships to be removed.
	query := selectForDelete

	if filter.ResourceType != "" {
		query = query.Where(sq.Eq{schema.ColNamespace: filter.ResourceType})
	}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{schema.ColObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{schema.ColRelation: filter.OptionalRelation})
	}
	if filter.OptionalResourceIdPrefix != "" {
		if strings.Contains(filter.OptionalResourceIdPrefix, "%") {
			return 0, false, fmt.Errorf("unable to delete relationships with a prefix containing the %% character")
		}

		query = query.Where(sq.Like{schema.ColObjectID: filter.OptionalResourceIdPrefix + "%"})
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{schema.ColUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{schema.ColUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{schema.ColUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	query = query.Limit(limit)

	selectSQL, args, err := query.ToSql()
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	args = append(args, rwt.newXID)

	// Construct a CTE to update the relationships as removed.
	cteSQL := fmt.Sprintf(
		"WITH found_tuples AS (%s)\nUPDATE %s SET %s = $%d WHERE (%s, %s, %s, %s, %s, %s, %s) IN (select * from found_tuples)",
		selectSQL,
		schema.TableTuple,
		schema.ColDeletedXid,
		len(args),
		schema.ColNamespace,
		schema.ColObjectID,
		schema.ColRelation,
		schema.ColUsersetNamespace,
		schema.ColUsersetObjectID,
		schema.ColUsersetRelation,
		schema.ColCreatedXid,
	)

	result, err := rwt.tx.Exec(ctx, cteSQL, args...)
	if err != nil {
		return 0, false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	numDeleted, err := safecast.ToUint64(result.RowsAffected())
	if err != nil {
		return 0, false, fmt.Errorf("unable to cast rows affected to uint64: %w", err)
	}

	return numDeleted, result.RowsAffected() == intLimit, nil
}

func (rwt *pgReadWriteTXN) deleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) (uint64, error) {
	// Add clauses for the ResourceFilter
	query := deleteTuple
	if filter.ResourceType != "" {
		query = query.Where(sq.Eq{schema.ColNamespace: filter.ResourceType})
	}
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{schema.ColObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{schema.ColRelation: filter.OptionalRelation})
	}
	if filter.OptionalResourceIdPrefix != "" {
		if strings.Contains(filter.OptionalResourceIdPrefix, "%") {
			return 0, fmt.Errorf("unable to delete relationships with a prefix containing the %% character")
		}

		query = query.Where(sq.Like{schema.ColObjectID: filter.OptionalResourceIdPrefix + "%"})
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{schema.ColUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{schema.ColUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{schema.ColUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	sql, args, err := query.Set(schema.ColDeletedXid, rwt.newXID).ToSql()
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	result, err := rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	numDeleted, err := safecast.ToUint64(result.RowsAffected())
	if err != nil {
		return 0, fmt.Errorf("unable to cast rows affected to uint64: %w", err)
	}

	return numDeleted, nil
}

func (rwt *pgReadWriteTXN) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	deletedNamespaceClause := sq.Or{}
	writeQuery := writeNamespace

	for _, newNamespace := range newConfigs {
		serialized, err := newNamespace.MarshalVT()
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{schema.ColNamespace: newNamespace.Name})

		valuesToWrite := []interface{}{newNamespace.Name, serialized}

		writeQuery = writeQuery.Values(valuesToWrite...)
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(schema.ColDeletedXid, rwt.newXID).
		Where(deletedNamespaceClause).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	sql, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	if _, err = rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	aliveFilter := func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(sq.Eq{schema.ColDeletedXid: liveDeletedTxnID})
	}

	nsClauses := make([]sq.Sqlizer, 0, len(nsNames))
	tplClauses := make([]sq.Sqlizer, 0, len(nsNames))
	querier := pgxcommon.QuerierFuncsFor(rwt.tx)
	for _, nsName := range nsNames {
		_, _, err := rwt.loadNamespace(ctx, nsName, querier, aliveFilter)
		switch {
		case errors.As(err, &datastore.NamespaceNotFoundError{}):
			return err

		case err == nil:
			nsClauses = append(nsClauses, sq.Eq{schema.ColNamespace: nsName})
			tplClauses = append(tplClauses, sq.Eq{schema.ColNamespace: nsName})

		default:
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(schema.ColDeletedXid, rwt.newXID).
		Where(sq.Or(nsClauses)).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := deleteNamespaceTuples.
		Set(schema.ColDeletedXid, rwt.newXID).
		Where(sq.Or(tplClauses)).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) RegisterCounter(ctx context.Context, name string, filter *core.RelationshipFilter) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) != 0 {
		return datastore.NewCounterAlreadyRegisteredErr(name, filter)
	}

	serializedFilter, err := filter.MarshalVT()
	if err != nil {
		return fmt.Errorf("unable to serialize filter: %w", err)
	}

	writeQuery := writeRelationshipCounter
	writeQuery = writeQuery.Values(name, serializedFilter, 0, nil)

	sql, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteRelationshipsCounter, err)
	}

	if _, err = rwt.tx.Exec(ctx, sql, args...); err != nil {
		// If this is a constraint violation, return that the filter is already registered.
		if pgxcommon.IsConstraintFailureError(err) {
			return datastore.NewCounterAlreadyRegisteredErr(name, filter)
		}

		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) UnregisterCounter(ctx context.Context, name string) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	deleteQuery := deleteRelationshipCounter.Where(sq.Eq{schema.ColCounterName: name})

	delSQL, delArgs, err := deleteQuery.
		Set(schema.ColDeletedXid, rwt.newXID).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.Exec(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) StoreCounterValue(ctx context.Context, name string, value int, computedAtRevision datastore.Revision) error {
	// Ensure the counter exists.
	counters, err := rwt.lookupCounters(ctx, name)
	if err != nil {
		return err
	}

	if len(counters) == 0 {
		return datastore.NewCounterNotRegisteredErr(name)
	}

	computedAtRevisionSnapshot := computedAtRevision.(postgresRevision).snapshot

	// Update the counter.
	updateQuery := updateRelationshipCounter.
		Set(schema.ColCounterCurrentCount, value).
		Set(schema.ColCounterSnapshot, computedAtRevisionSnapshot)

	sql, args, err := updateQuery.
		Where(sq.Eq{schema.ColCounterName: name}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteRelationshipsCounter, err)
	}

	if _, err = rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errUnableToWriteRelationshipsCounter, err)
	}

	return nil
}

var copyCols = []string{
	schema.ColNamespace,
	schema.ColObjectID,
	schema.ColRelation,
	schema.ColUsersetNamespace,
	schema.ColUsersetObjectID,
	schema.ColUsersetRelation,
	schema.ColCaveatContextName,
	schema.ColCaveatContext,
	schema.ColExpiration,
}

func (rwt *pgReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return pgxcommon.BulkLoad(ctx, rwt.tx, schema.TableTuple, copyCols, iter)
}

func exactRelationshipClause(r tuple.Relationship) sq.Eq {
	return sq.Eq{
		schema.ColNamespace:        r.Resource.ObjectType,
		schema.ColObjectID:         r.Resource.ObjectID,
		schema.ColRelation:         r.Resource.Relation,
		schema.ColUsersetNamespace: r.Subject.ObjectType,
		schema.ColUsersetObjectID:  r.Subject.ObjectID,
		schema.ColUsersetRelation:  r.Subject.Relation,
	}
}

func exactRelationshipDifferentCaveatAndExpirationClause(r tuple.Relationship) sq.And {
	var caveatName string
	var caveatContext map[string]any
	if r.OptionalCaveat != nil {
		caveatName = r.OptionalCaveat.CaveatName
		caveatContext = r.OptionalCaveat.Context.AsMap()
	}

	expiration := r.OptionalExpiration
	return sq.And{
		sq.Eq{
			schema.ColNamespace:        r.Resource.ObjectType,
			schema.ColObjectID:         r.Resource.ObjectID,
			schema.ColRelation:         r.Resource.Relation,
			schema.ColUsersetNamespace: r.Subject.ObjectType,
			schema.ColUsersetObjectID:  r.Subject.ObjectID,
			schema.ColUsersetRelation:  r.Subject.Relation,
		},
		sq.Or{
			sq.Expr(fmt.Sprintf(`%s IS DISTINCT FROM ?`, schema.ColCaveatContextName), caveatName),
			sq.Expr(fmt.Sprintf(`%s IS DISTINCT FROM ?`, schema.ColExpiration), expiration),
			sq.NotEq{
				schema.ColCaveatContext: caveatContext,
			},
		},
	}
}

var _ datastore.ReadWriteTransaction = &pgReadWriteTXN{}
