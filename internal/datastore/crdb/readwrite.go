package crdb

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/proto"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
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
	upsertTupleSuffix = fmt.Sprintf(
		"ON CONFLICT (%s,%s,%s,%s,%s,%s) DO UPDATE SET %s = now(), %s = excluded.%s, %s = excluded.%s WHERE (relation_tuple.%s <> excluded.%s OR relation_tuple.%s <> excluded.%s)",
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
		colCaveatContextName,
		colCaveatContextName,
		colCaveatContext,
		colCaveatContext,
	)

	queryWriteTuple = psql.Insert(tableTuple).Columns(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCaveatContextName,
		colCaveatContext,
	)

	queryTouchTuple = queryWriteTuple.Suffix(upsertTupleSuffix)

	queryDeleteTuples = psql.Delete(tableTuple)

	queryTouchTransaction = fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES ($1::text) ON CONFLICT (%s) DO UPDATE SET %s = now()",
		tableTransactions,
		colTransactionKey,
		colTransactionKey,
		colTimestamp,
	)
)

func (rwt *crdbReadWriteTXN) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	bulkWrite := queryWriteTuple
	var bulkWriteCount int64

	bulkTouch := queryTouchTuple
	var bulkTouchCount int64

	// Process the actual updates
	for _, mutation := range mutations {
		rel := mutation.Tuple

		var caveatContext map[string]any
		var caveatName string
		if rel.Caveat != nil {
			caveatName = rel.Caveat.CaveatName
			caveatContext = rel.Caveat.Context.AsMap()
		}

		rwt.addOverlapKey(rel.ResourceAndRelation.Namespace)
		rwt.addOverlapKey(rel.Subject.Namespace)

		switch mutation.Operation {
		case core.RelationTupleUpdate_TOUCH:
			rwt.relCountChange++
			bulkTouch = bulkTouch.Values(
				rel.ResourceAndRelation.Namespace,
				rel.ResourceAndRelation.ObjectId,
				rel.ResourceAndRelation.Relation,
				rel.Subject.Namespace,
				rel.Subject.ObjectId,
				rel.Subject.Relation,
				caveatName,
				caveatContext,
			)
			bulkTouchCount++
		case core.RelationTupleUpdate_CREATE:
			rwt.relCountChange++
			bulkWrite = bulkWrite.Values(
				rel.ResourceAndRelation.Namespace,
				rel.ResourceAndRelation.ObjectId,
				rel.ResourceAndRelation.Relation,
				rel.Subject.Namespace,
				rel.Subject.ObjectId,
				rel.Subject.Relation,
				caveatName,
				caveatContext,
			)
			bulkWriteCount++
		case core.RelationTupleUpdate_DELETE:
			rwt.relCountChange--
			sql, args, err := queryDeleteTuples.Where(exactRelationshipClause(rel)).ToSql()
			if err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}

			if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
		default:
			log.Ctx(ctx).Error().Stringer("operation", mutation.Operation).Msg("unknown operation type")
			return fmt.Errorf("unknown mutation operation: %s", mutation.Operation)
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
			// If a unique constraint violation is returned, then its likely that the cause
			// was an existing relationship given as a CREATE.
			if cerr := pgxcommon.ConvertToWriteConstraintError(livingTupleConstraint, err); cerr != nil {
				return cerr
			}

			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
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

func (rwt *crdbReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	// Add clauses for the ResourceFilter
	query := queryDeleteTuples.Where(sq.Eq{colNamespace: filter.ResourceType})
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
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
	sql, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	modified, err := rwt.tx.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	rwt.relCountChange -= modified.RowsAffected()

	return nil
}

func (rwt *crdbReadWriteTXN) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	query := queryWriteNamespace

	for _, newConfig := range newConfigs {
		rwt.addOverlapKey(newConfig.Name)

		serialized, err := proto.Marshal(newConfig)
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
			if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
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

	deleteTupleSQL, deleteTupleArgs, err := queryDeleteTuples.Where(sq.Or(tplClauses)).ToSql()
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
}

func (rwt *crdbReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	return pgxcommon.BulkLoad(ctx, rwt.tx, tableTuple, copyCols, iter)
}

var _ datastore.ReadWriteTransaction = &crdbReadWriteTXN{}
