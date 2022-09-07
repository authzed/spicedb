package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/google/uuid"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type spannerReadWriteTXN struct {
	spannerReader
	ctx        context.Context
	spannerRWT *spanner.ReadWriteTransaction
}

func (rwt spannerReadWriteTXN) WriteRelationships(mutations []*core.RelationTupleUpdate) error {
	ctx, span := tracer.Start(rwt.ctx, "WriteRelationships")
	defer span.End()

	changeUUID := uuid.New().String()

	var rowCountChange int64

	for _, mutation := range mutations {
		var txnMut *spanner.Mutation
		var op int
		switch mutation.Operation {
		case core.RelationTupleUpdate_TOUCH:
			rowCountChange++
			txnMut = spanner.InsertOrUpdate(tableRelationship, allRelationshipCols, upsertVals(mutation.Tuple))
			op = colChangeOpTouch
		case core.RelationTupleUpdate_CREATE:
			rowCountChange++
			txnMut = spanner.Insert(tableRelationship, allRelationshipCols, upsertVals(mutation.Tuple))
			op = colChangeOpCreate
		case core.RelationTupleUpdate_DELETE:
			rowCountChange--
			txnMut = spanner.Delete(tableRelationship, keyFromRelationship(mutation.Tuple))
			op = colChangeOpDelete
		default:
			log.Ctx(ctx).Error().Stringer("operation", mutation.Operation).Msg("unknown operation type")
			return fmt.Errorf(
				errUnableToWriteRelationships,
				fmt.Errorf("unknown mutation operation: %s", mutation.Operation),
			)
		}

		changelogMut := spanner.Insert(tableChangelog, allChangelogCols, changeVals(changeUUID, op, mutation.Tuple))
		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{txnMut, changelogMut}); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	if err := updateCounter(ctx, rwt.spannerRWT, rowCountChange); err != nil {
		return fmt.Errorf(errUnableToWriteRelationships, err)
	}

	return nil
}

func (rwt spannerReadWriteTXN) DeleteRelationships(filter *v1.RelationshipFilter) error {
	ctx, span := tracer.Start(rwt.ctx, "DeleteRelationships")
	defer span.End()

	err := deleteWithFilter(ctx, rwt.spannerRWT, filter)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}
	return nil
}

type selectAndDelete struct {
	sel sq.SelectBuilder
	del sq.DeleteBuilder
}

func (snd selectAndDelete) Where(pred interface{}, args ...interface{}) selectAndDelete {
	snd.sel = snd.sel.Where(pred, args...)
	snd.del = snd.del.Where(pred, args...)
	return snd
}

func deleteWithFilter(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter) error {
	queries := selectAndDelete{queryTuples, sql.Delete(tableRelationship)}

	// Add clauses for the ResourceFilter
	queries = queries.Where(sq.Eq{colNamespace: filter.ResourceType})
	if filter.OptionalResourceId != "" {
		queries = queries.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		queries = queries.Where(sq.Eq{colRelation: filter.OptionalRelation})
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		queries = queries.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			queries = queries.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			queries = queries.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	ssql, sargs, err := queries.sel.ToSql()
	if err != nil {
		return err
	}

	toDelete := rwt.Query(ctx, statementFromSQL(ssql, sargs))

	changeUUID := uuid.NewString()

	// Pre-allocate a single relationship
	rel := core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{},
		Subject:             &core.ObjectAndRelation{},
	}

	var changelogMutations []*spanner.Mutation
	if err := toDelete.Do(func(row *spanner.Row) error {
		err := row.Columns(
			&rel.ResourceAndRelation.Namespace,
			&rel.ResourceAndRelation.ObjectId,
			&rel.ResourceAndRelation.Relation,
			&rel.Subject.Namespace,
			&rel.Subject.ObjectId,
			&rel.Subject.Relation,
		)
		if err != nil {
			return err
		}

		changelogMutations = append(changelogMutations, spanner.Insert(
			tableChangelog,
			allChangelogCols,
			changeVals(changeUUID, colChangeOpDelete, &rel),
		))
		return nil
	}); err != nil {
		return err
	}

	if err := rwt.BufferWrite(changelogMutations); err != nil {
		return err
	}

	sql, args, err := queries.del.ToSql()
	if err != nil {
		return err
	}

	numDeleted, err := rwt.Update(ctx, statementFromSQL(sql, args))
	if err != nil {
		return err
	}

	if err := updateCounter(ctx, rwt, -1*numDeleted); err != nil {
		return err
	}

	return nil
}

func upsertVals(r *core.RelationTuple) []interface{} {
	key := keyFromRelationship(r)
	return append(key, spanner.CommitTimestamp)
}

func keyFromRelationship(r *core.RelationTuple) spanner.Key {
	return spanner.Key{
		r.ResourceAndRelation.Namespace,
		r.ResourceAndRelation.ObjectId,
		r.ResourceAndRelation.Relation,
		r.Subject.Namespace,
		r.Subject.ObjectId,
		r.Subject.Relation,
	}
}

func changeVals(changeUUID string, op int, r *core.RelationTuple) []interface{} {
	return []interface{}{
		spanner.CommitTimestamp,
		changeUUID,
		op,
		r.ResourceAndRelation.Namespace,
		r.ResourceAndRelation.ObjectId,
		r.ResourceAndRelation.Relation,
		r.Subject.Namespace,
		r.Subject.ObjectId,
		r.Subject.Relation,
	}
}

func (rwt spannerReadWriteTXN) WriteNamespaces(newConfigs ...*core.NamespaceDefinition) error {
	_, span := tracer.Start(rwt.ctx, "WriteNamespace")
	defer span.End()

	mutations := make([]*spanner.Mutation, 0, len(newConfigs))
	for _, newConfig := range newConfigs {
		serialized, err := proto.Marshal(newConfig)
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		mutations = append(mutations, spanner.InsertOrUpdate(
			tableNamespace,
			[]string{colNamespaceName, colNamespaceConfig, colTimestamp},
			[]interface{}{newConfig.Name, serialized, spanner.CommitTimestamp},
		))
	}

	return rwt.spannerRWT.BufferWrite(mutations)
}

func (rwt spannerReadWriteTXN) DeleteNamespace(nsName string) error {
	ctx, span := tracer.Start(rwt.ctx, "DeleteNamespace")
	defer span.End()

	if err := deleteWithFilter(ctx, rwt.spannerRWT, &v1.RelationshipFilter{
		ResourceType: nsName,
	}); err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{
		spanner.Delete(tableNamespace, spanner.KeySetFromKeys(spanner.Key{nsName})),
	})
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return err
}

var _ datastore.ReadWriteTransaction = spannerReadWriteTXN{}
