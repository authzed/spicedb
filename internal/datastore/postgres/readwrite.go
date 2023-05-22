package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/authzed/spicedb/pkg/spiceerrors"

	"github.com/authzed/spicedb/pkg/tuple"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/proto"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

	deleteTuple = psql.Update(tableTuple).Where(sq.Eq{colDeletedXid: liveDeletedTxnID})
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

func (rwt *pgReadWriteTXN) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	touchMutationsByNonCaveat := make(map[string]*core.RelationTupleUpdate, len(mutations))
	hasCreateInserts := false

	createInserts := writeTuple
	touchInserts := writeTuple
	deleteClauses := sq.Or{}

	// Parse the updates, building inserts for CREATE/TOUCH and deletes for DELETE.
	for _, mut := range mutations {
		tpl := mut.Tuple

		switch mut.Operation {
		case core.RelationTupleUpdate_CREATE:
			createInserts = appendForInsertion(createInserts, tpl)
			hasCreateInserts = true

		case core.RelationTupleUpdate_TOUCH:
			touchMutationsByNonCaveat[tuple.StringWithoutCaveat(tpl)] = mut
			touchInserts = appendForInsertion(touchInserts, tpl)

		case core.RelationTupleUpdate_DELETE:
			deleteClauses = append(deleteClauses, exactRelationshipClause(tpl))

		default:
			return spiceerrors.MustBugf("unknown tuple mutation: %v", mut)
		}
	}

	// Run CREATE insertions, if any.
	if hasCreateInserts {
		sql, args, err := createInserts.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.Exec(ctx, sql, args...)
		if err != nil {
			// If a unique constraint violation is returned, then its likely that the cause
			// was an existing relationship given as a CREATE.
			if cerr := pgxcommon.ConvertToWriteConstraintError(livingTupleConstraint, err); cerr != nil {
				return cerr
			}

			return fmt.Errorf(errUnableToWriteRelationships, err)
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
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		rows, err := rwt.tx.Query(ctx, sql, args...)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
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
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}

			tplString := tuple.StringWithoutCaveat(tpl)
			_, ok := touchMutationsByNonCaveat[tplString]
			if !ok {
				return spiceerrors.MustBugf("missing expected completed TOUCH mutation")
			}

			delete(touchMutationsByNonCaveat, tplString)
		}
		rows.Close()

		// For each remaining TOUCH mutation, add a DELETE operation for the row iff the caveat and/or
		// context has changed. For ones in which the caveat name and/or context did not change, there is
		// no need to replace the row, as it is already present.
		for _, mut := range touchMutationsByNonCaveat {
			deleteClauses = append(deleteClauses, exactRelationshipDifferentCaveatClause(mut.Tuple))
		}
	}

	// Execute the DELETE operation for any DELETE mutations or TOUCH mutations that matched existing
	// relationships and whose caveat name or context is different in some manner. We use RETURNING
	// to determine which TOUCHed relationships were deleted by virtue of their caveat name and/or
	// context being changed.
	if len(deleteClauses) == 0 {
		// Nothing more to do.
		return nil
	}

	builder := deleteTuple.
		Where(deleteClauses).
		Suffix(fmt.Sprintf("RETURNING %s, %s, %s, %s, %s, %s",
			colNamespace,
			colObjectID,
			colRelation,
			colUsersetNamespace,
			colUsersetObjectID,
			colUsersetRelation,
		))

	sql, args, err := builder.
		Set(colDeletedXid, rwt.newXID).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteRelationships, err)
	}

	rows, err := rwt.tx.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteRelationships, err)
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
		err := rows.Scan(
			&deletedTpl.ResourceAndRelation.Namespace,
			&deletedTpl.ResourceAndRelation.ObjectId,
			&deletedTpl.ResourceAndRelation.Relation,
			&deletedTpl.Subject.Namespace,
			&deletedTpl.Subject.ObjectId,
			&deletedTpl.Subject.Relation,
		)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		tplString := tuple.StringWithoutCaveat(deletedTpl)
		mutation, ok := touchMutationsByNonCaveat[tplString]
		if !ok {
			// This did not represent a TOUCH operation.
			continue
		}

		touchWrite = appendForInsertion(touchWrite, mutation.Tuple)
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
		return fmt.Errorf(errUnableToWriteRelationships, err)
	}

	_, err = rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteRelationships, err)
	}

	return nil
}

func (rwt *pgReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	// Add clauses for the ResourceFilter
	query := deleteTuple.Where(sq.Eq{colNamespace: filter.ResourceType})
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
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
			break
		default:
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}

		nsClauses = append(nsClauses, sq.Eq{colNamespace: nsName})
		tplClauses = append(tplClauses, sq.Eq{colNamespace: nsName})
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
