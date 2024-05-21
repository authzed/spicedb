package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/typesystem"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/proto"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	errUnableToWriteConfig         = "unable to write namespace config: %w"
	errUnableToDeleteConfig        = "unable to delete namespace config: %w"
	errUnableToWriteRelationships  = "unable to write relationships: %w"
	errUnableToDeleteRelationships = "unable to delete relationships: %w"
)

var (
	writeNamespace = psql.Insert(tableNamespace).Columns(
		colNamespace,
		colConfig,
	)

	deleteNamespace = psql.Update(tableNamespace).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})

	deleteNamespaceTuples = psql.Update(tableTuple).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})

	writeTuple = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
	)

	deleteTuple     = psql.Update(tableTuple).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
	selectForDelete = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedXid,
	).From(tableTuple).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
)

type pgReadWriteTXN struct {
	*pgReader
	tx     pgx.Tx
	newXID xid8
}

func appendForInsertion(builder sq.InsertBuilder, tpl *core.RelationTuple) sq.InsertBuilder {
	var caveatName string
	var caveatContext map[string]any
	if tpl.Caveat != nil {
		caveatName = tpl.Caveat.CaveatName
		caveatContext = tpl.Caveat.Context.AsMap()
	}

	valuesToWrite := []interface{}{
		tpl.ResourceAndRelation.Namespace,
		tpl.ResourceAndRelation.ObjectId,
		tpl.ResourceAndRelation.Relation,
		tpl.Subject.Namespace,
		tpl.Subject.ObjectId,
		tpl.Subject.Relation,
		caveatName,
		caveatContext, // PGX driver serializes map[string]any to JSONB type columns
	}

	return builder.Values(valuesToWrite...)
}

func (rwt *pgReadWriteTXN) collectSimplifiedTouchTypes(ctx context.Context, mutations []*core.RelationTupleUpdate) (*mapz.Set[string], error) {
	// Collect the list of namespaces used for resources for relationships being TOUCHed.
	touchedResourceNamespaces := mapz.NewSet[string]()
	for _, mut := range mutations {
		if mut.Operation == core.RelationTupleUpdate_TOUCH {
			touchedResourceNamespaces.Add(mut.Tuple.ResourceAndRelation.Namespace)
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
		return nil, fmt.Errorf(errUnableToWriteRelationships, err)
	}

	if len(namespaces) == 0 {
		return relationSupportSimplifiedTouch, nil
	}

	nsDefByName := make(map[string]*core.NamespaceDefinition, len(namespaces))
	for _, ns := range namespaces {
		nsDefByName[ns.Definition.Name] = ns.Definition
	}

	for _, mut := range mutations {
		tpl := mut.Tuple

		if mut.Operation != core.RelationTupleUpdate_TOUCH {
			continue
		}

		nsDef, ok := nsDefByName[tpl.ResourceAndRelation.Namespace]
		if !ok {
			continue
		}

		vts, err := typesystem.NewNamespaceTypeSystem(nsDef, typesystem.ResolverForDatastoreReader(rwt))
		if err != nil {
			return nil, fmt.Errorf(errUnableToWriteRelationships, err)
		}

		notAllowed, err := vts.RelationDoesNotAllowCaveatsForSubject(tpl.ResourceAndRelation.Relation, tpl.Subject.Namespace)
		if err != nil {
			// Ignore errors and just fallback to the less efficient path.
			continue
		}

		if notAllowed {
			relationSupportSimplifiedTouch.Add(nsDef.Name + "#" + tpl.ResourceAndRelation.Relation + "@" + tpl.Subject.Namespace)
			continue
		}
	}

	return relationSupportSimplifiedTouch, nil
}

func (rwt *pgReadWriteTXN) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate, returnStatus bool) ([]*core.RelationTupleUpdateStatus, error) {
	touchMutationsByNonCaveat := make(map[string]*core.RelationTupleUpdate, len(mutations))
	hasCreateInserts := false

	createInserts := writeTuple
	touchInserts := writeTuple

	deleteClauses := sq.Or{}

	var statusMap map[string]*core.RelationTupleUpdateStatus
	var statuses []*core.RelationTupleUpdateStatus
	if returnStatus {
		// If we are returning status, we need to track the status of each mutation.
		// We will use a map to track the status of each mutation by the tuple string.
		// We will also use a slice to track the order of the statuses.
		// The slice will never be mutated, but the map will be updated as we process, however they both
		// use pointers to the same status objects, therefore modifying the map also modifies the slice.
		statusMap = make(map[string]*core.RelationTupleUpdateStatus, len(mutations))
		statuses = make([]*core.RelationTupleUpdateStatus, 0, len(mutations))
	}

	// Determine the set of relation+subject types for whom a "simplified" TOUCH operation can be used. A
	// simplified TOUCH operation is one in which the relationship does not support caveats for the subject
	// type. In such cases, the "DELETE" operation is unnecessary because the relationship does not support
	// caveats for the subject type, and thus the relationship can be TOUCHed without needing to check for
	// the existence of a relationship with a different caveat name and/or context which might need to be
	// replaced.
	relationSupportSimplifiedTouch, err := rwt.collectSimplifiedTouchTypes(ctx, mutations)
	if err != nil {
		return nil, err
	}

	// Parse the updates, building inserts for CREATE/TOUCH and deletes for DELETE.
	for _, mut := range mutations {
		tpl := mut.Tuple

		switch mut.Operation {
		case core.RelationTupleUpdate_CREATE:
			createInserts = appendForInsertion(createInserts, tpl)
			hasCreateInserts = true

			if returnStatus {
				// For a CREATE operation we can just return the original tuple with a "CREATED" status.
				// This is because the operation fails in all other scenarios.
				status := &core.RelationTupleUpdateStatus{
					Status: &core.RelationTupleUpdateStatus_Created{
						Created: tpl,
					},
				}
				statusMap[tuple.StringWithoutCaveat(tpl)] = status
				statuses = append(statuses, status)
			}

		case core.RelationTupleUpdate_TOUCH:
			touchInserts = appendForInsertion(touchInserts, tpl)
			touchMutationsByNonCaveat[tuple.StringWithoutCaveat(tpl)] = mut

			if returnStatus {
				// For a TOUCH operation we initialize a "NOOP" status with the original tuple.
				// We will update the status later if the operation was an insert or a replacement.
				status := &core.RelationTupleUpdateStatus{
					Status: &core.RelationTupleUpdateStatus_Noop{
						Noop: tpl,
					},
				}
				statusMap[tuple.StringWithoutCaveat(tpl)] = status
				statuses = append(statuses, status)
			}

		case core.RelationTupleUpdate_DELETE:
			deleteClauses = append(deleteClauses, exactRelationshipClause(tpl))

			if returnStatus {
				// For a DELETE operation we initialize a "NOOP" status with the original tuple.
				// We will update the status later if the tuple was actually deleted.
				status := &core.RelationTupleUpdateStatus{
					Status: &core.RelationTupleUpdateStatus_Noop{
						Noop: tpl,
					},
				}
				statusMap[tuple.StringWithoutCaveat(tpl)] = status
				statuses = append(statuses, status)
			}

		default:
			return nil, spiceerrors.MustBugf("unknown tuple mutation: %v", mut)
		}
	}

	// Run CREATE insertions, if any.
	if hasCreateInserts {
		sql, args, err := createInserts.ToSql()
		if err != nil {
			return nil, fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.Exec(ctx, sql, args...)
		if err != nil {
			return nil, fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	// For each of the TOUCH operations, invoke the INSERTs, but with `ON CONFLICT DO NOTHING` to ensure
	// that the operations over existing relationships no-op.
	if len(touchMutationsByNonCaveat) > 0 {
		touchInserts = touchInserts.Suffix(fmt.Sprintf("ON CONFLICT DO NOTHING RETURNING %s, %s, %s, %s, %s, %s",
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
		))

		sql, args, err := touchInserts.ToSql()
		if err != nil {
			return nil, fmt.Errorf(errUnableToWriteRelationships, err)
		}

		rows, err := rwt.tx.Query(ctx, sql, args...)
		if err != nil {
			return nil, fmt.Errorf(errUnableToWriteRelationships, err)
		}
		defer rows.Close()

		// Remove from the TOUCH map of operations each row that was successfully inserted.
		// This will cover any TOUCH that created an entirely new relationship, acting like
		// a CREATE.
		tpl := &core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}

		for rows.Next() {
			err := rows.Scan(
				&tpl.ResourceAndRelation.Namespace,
				&tpl.ResourceAndRelation.ObjectId,
				&tpl.ResourceAndRelation.Relation,
				&tpl.Subject.Namespace,
				&tpl.Subject.ObjectId,
				&tpl.Subject.Relation,
			)
			if err != nil {
				return nil, fmt.Errorf(errUnableToWriteRelationships, err)
			}

			tplString := tuple.StringWithoutCaveat(tpl)
			original, ok := touchMutationsByNonCaveat[tplString]
			if !ok {
				return nil, spiceerrors.MustBugf("missing expected completed TOUCH mutation")
			}

			if returnStatus {
				// If the tuple was inserted, then update the status to "CREATED".
				// The tuple referenced is the original tuple from the map, not the one returned from the query
				// as the original one might contain a caveat that the one returned from the query does not.
				statusMap[tplString].Status = &core.RelationTupleUpdateStatus_Created{
					Created: original.Tuple,
				}
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
			if relationSupportSimplifiedTouch.Has(mut.Tuple.ResourceAndRelation.Namespace + "#" + mut.Tuple.ResourceAndRelation.Relation + "@" + mut.Tuple.Subject.Namespace) {
				continue
			}

			deleteClauses = append(deleteClauses, exactRelationshipDifferentCaveatClause(mut.Tuple))
		}
	}

	// Execute the "DELETE" operation (an UPDATE with setting the deletion ID to the current transaction ID)
	// for any DELETE mutations or TOUCH mutations that matched existing relationships and whose caveat name
	// or context is different in some manner. We use RETURNING to determine which TOUCHed relationships were
	// deleted by virtue of their caveat name and/or context being changed.
	if len(deleteClauses) == 0 {
		// Nothing more to do.
		return statuses, nil
	}

	builder := deleteTuple.
		Where(deleteClauses)

	if returnStatus {
		// If we are returning status, we need to return the old caveat name and context.
		// This is so that we can populate the `old` field of the "UPDATED" status.
		builder = builder.Suffix(fmt.Sprintf("RETURNING %s, %s, %s, %s, %s, %s, %s, %s",
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
			colCaveatContextName,
			colCaveatContext,
		))
	} else {
		builder = builder.Suffix(fmt.Sprintf("RETURNING %s, %s, %s, %s, %s, %s",
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
		))
	}

	sql, args, err := builder.
		Set(colDeletedXid, rwt.newXID).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf(errUnableToWriteRelationships, err)
	}

	rows, err := rwt.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errUnableToWriteRelationships, err)
	}
	defer rows.Close()

	// For each deleted row representing a TOUCH, recreate with the new caveat and/or context.
	touchWrite := writeTuple
	touchWriteHasValues := false

	deletedTpl := &core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{},
		Subject:             &core.ObjectAndRelation{},
	}

	for rows.Next() {
		if returnStatus {
			// If we are returning statuses, then we are redefining `deletedTpl` for each row for 2 reasons:
			// 1. To ensure that we have a new variable for each row, so that we can safely use it in the status map,
			//    otherwise all the "UPDATED" statuses would reference the same `Old` tuple pointer.
			// 2. To populate the old caveat name and context which will be used in the "UPDATED" status.
			deletedTpl = &core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{},
				Subject:             &core.ObjectAndRelation{},
				Caveat:              &core.ContextualizedCaveat{},
			}
		}

		dest := []interface{}{
			&deletedTpl.ResourceAndRelation.Namespace,
			&deletedTpl.ResourceAndRelation.ObjectId,
			&deletedTpl.ResourceAndRelation.Relation,
			&deletedTpl.Subject.Namespace,
			&deletedTpl.Subject.ObjectId,
			&deletedTpl.Subject.Relation,
		}

		if returnStatus {
			dest = append(dest,
				&deletedTpl.Caveat.CaveatName,
				&deletedTpl.Caveat.Context,
			)
		}

		err := rows.Scan(dest...)
		if err != nil {
			return nil, fmt.Errorf(errUnableToWriteRelationships, err)
		}

		if returnStatus && deletedTpl.Caveat.CaveatName == "" {
			// If the caveat name is empty, then remove the `Caveat` field from the tuple.
			deletedTpl.Caveat = nil
		}

		tplString := tuple.StringWithoutCaveat(deletedTpl)
		mutation, ok := touchMutationsByNonCaveat[tplString]
		if !ok {
			// This did not represent a TOUCH operation.
			if returnStatus {
				statusMap[tplString].Status = &core.RelationTupleUpdateStatus_Deleted{
					Deleted: deletedTpl,
				}
			}

			continue
		}

		touchWrite = appendForInsertion(touchWrite, mutation.Tuple)
		touchWriteHasValues = true

		if returnStatus {
			statusMap[tplString].Status = &core.RelationTupleUpdateStatus_Updated{
				Updated: &core.RelationTupleUpdateStatus_Update{
					Old: deletedTpl,
					New: mutation.Tuple,
				},
			}
		}
	}
	rows.Close()

	// If no INSERTs are necessary to update caveats, then nothing more to do.
	if !touchWriteHasValues {
		return statuses, nil
	}

	// Otherwise execute the INSERTs for the caveated-changes TOUCHed relationships.
	sql, args, err = touchWrite.ToSql()
	if err != nil {
		return nil, fmt.Errorf(errUnableToWriteRelationships, err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf(errUnableToWriteRelationships, err)
	}

	return statuses, nil
}

func (rwt *pgReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter, opts ...options.DeleteOptionsOption) (bool, error) {
	delOpts := options.NewDeleteOptionsWithOptionsAndDefaults(opts...)
	if delOpts.DeleteLimit != nil && *delOpts.DeleteLimit > 0 {
		return rwt.deleteRelationshipsWithLimit(ctx, filter, *delOpts.DeleteLimit)
	}

	return false, rwt.deleteRelationships(ctx, filter)
}

func (rwt *pgReadWriteTXN) deleteRelationshipsWithLimit(ctx context.Context, filter *v1.RelationshipFilter, limit uint64) (bool, error) {
	// Construct a select query for the relationships to be removed.
	query := selectForDelete

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

	query = query.Limit(limit)

	selectSQL, args, err := query.ToSql()
	if err != nil {
		return false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	args = append(args, rwt.newXID)

	// Construct a CTE to update the relationships as removed.
	cteSQL := fmt.Sprintf(
		"WITH found_tuples AS (%s)\nUPDATE %s SET %s = $%d WHERE (%s, %s, %s, %s, %s, %s, %s) IN (select * from found_tuples)",
		selectSQL,
		tableTuple,
		colDeletedXid,
		len(args),
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedXid,
	)

	result, err := rwt.tx.Exec(ctx, cteSQL, args...)
	if err != nil {
		return false, fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return result.RowsAffected() == int64(limit), nil
}

func (rwt *pgReadWriteTXN) deleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	// Add clauses for the ResourceFilter
	query := deleteTuple
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
			return fmt.Errorf("unable to delete relationships with a prefix containing the %% character")
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

	sql, args, err := query.Set(colDeletedXid, rwt.newXID).ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	deletedNamespaceClause := sq.Or{}
	writeQuery := writeNamespace

	for _, newNamespace := range newConfigs {
		serialized, err := proto.Marshal(newNamespace)
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{colNamespace: newNamespace.Name})

		valuesToWrite := []interface{}{newNamespace.Name, serialized}

		writeQuery = writeQuery.Values(valuesToWrite...)
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedXid, rwt.newXID).
		Where(sq.And{sq.Eq{colDeletedXid: liveDeletedTxnID}, deletedNamespaceClause}).
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
	filterer := func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
	}

	nsClauses := make([]sq.Sqlizer, 0, len(nsNames))
	tplClauses := make([]sq.Sqlizer, 0, len(nsNames))
	querier := pgxcommon.QuerierFuncsFor(rwt.tx)
	for _, nsName := range nsNames {
		_, _, err := rwt.loadNamespace(ctx, nsName, querier, filterer)
		switch {
		case errors.As(err, &datastore.ErrNamespaceNotFound{}):
			return err

		case err == nil:
			nsClauses = append(nsClauses, sq.Eq{colNamespace: nsName})
			tplClauses = append(tplClauses, sq.Eq{colNamespace: nsName})

		default:
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}
	}

	delSQL, delArgs, err := deleteNamespace.
		Set(colDeletedXid, rwt.newXID).
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
		Set(colDeletedXid, rwt.newXID).
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

var copyCols = []string{
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
	colCaveatContextName,
	colCaveatContext,
}

func (rwt *pgReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return pgxcommon.BulkLoad(ctx, rwt.tx, tableTuple, copyCols, iter)
}

func exactRelationshipClause(r *core.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        r.ResourceAndRelation.Namespace,
		colObjectID:         r.ResourceAndRelation.ObjectId,
		colRelation:         r.ResourceAndRelation.Relation,
		colUsersetNamespace: r.Subject.Namespace,
		colUsersetObjectID:  r.Subject.ObjectId,
		colUsersetRelation:  r.Subject.Relation,
	}
}

func exactRelationshipDifferentCaveatClause(r *core.RelationTuple) sq.And {
	var caveatName string
	var caveatContext map[string]any
	if r.Caveat != nil {
		caveatName = r.Caveat.CaveatName
		caveatContext = r.Caveat.Context.AsMap()
	}

	return sq.And{
		sq.Eq{
			colNamespace:        r.ResourceAndRelation.Namespace,
			colObjectID:         r.ResourceAndRelation.ObjectId,
			colRelation:         r.ResourceAndRelation.Relation,
			colUsersetNamespace: r.Subject.Namespace,
			colUsersetObjectID:  r.Subject.ObjectId,
			colUsersetRelation:  r.Subject.Relation,
		},
		sq.Or{
			sq.NotEq{
				colCaveatContextName: caveatName,
			},
			sq.NotEq{
				colCaveatContext: caveatContext,
			},
		},
	}
}

var _ datastore.ReadWriteTransaction = &pgReadWriteTXN{}
