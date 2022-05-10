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

func (rwt spannerReadWriteTXN) WriteRelationships(mutations []*v1.RelationshipUpdate) error {
	ctx, span := tracer.Start(rwt.ctx, "WriteTuples")
	defer span.End()

	changeUUID := uuid.New().String()

	var rowCountChange int64

	for _, mutation := range mutations {
		var txnMut *spanner.Mutation
		var op int
		switch mutation.Operation {
		case v1.RelationshipUpdate_OPERATION_TOUCH:
			rowCountChange++
			txnMut = spanner.InsertOrUpdate(tableRelationship, allRelationshipCols, upsertVals(mutation.Relationship))
			op = colChangeOpTouch
		case v1.RelationshipUpdate_OPERATION_CREATE:
			rowCountChange++
			txnMut = spanner.Insert(tableRelationship, allRelationshipCols, upsertVals(mutation.Relationship))
			op = colChangeOpCreate
		case v1.RelationshipUpdate_OPERATION_DELETE:
			rowCountChange--
			txnMut = spanner.Delete(tableRelationship, keyFromRelationship(mutation.Relationship))
			op = colChangeOpDelete
		default:
			log.Ctx(ctx).Error().Stringer("operation", mutation.Operation).Msg("unknown operation type")
			return fmt.Errorf(
				errUnableToWriteRelationships,
				fmt.Errorf("unknown mutation operation: %s", mutation.Operation),
			)
		}

		changelogMut := spanner.Insert(tableChangelog, allChangelogCols, changeVals(changeUUID, op, mutation.Relationship))
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
	rel := v1.Relationship{
		Resource: &v1.ObjectReference{},
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{},
		},
	}

	var changelogMutations []*spanner.Mutation
	if err := toDelete.Do(func(row *spanner.Row) error {
		err := row.Columns(
			&rel.Resource.ObjectType,
			&rel.Resource.ObjectId,
			&rel.Relation,
			&rel.Subject.Object.ObjectType,
			&rel.Subject.Object.ObjectId,
			&rel.Subject.OptionalRelation,
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

func upsertVals(r *v1.Relationship) []interface{} {
	key := keyFromRelationship(r)
	return append(key, spanner.CommitTimestamp)
}

func keyFromRelationship(r *v1.Relationship) spanner.Key {
	return spanner.Key{
		r.Resource.ObjectType,
		r.Resource.ObjectId,
		r.Relation,
		r.Subject.Object.ObjectType,
		r.Subject.Object.ObjectId,
		stringz.DefaultEmpty(r.Subject.OptionalRelation, datastore.Ellipsis),
	}
}

func changeVals(changeUUID string, op int, r *v1.Relationship) []interface{} {
	return []interface{}{
		spanner.CommitTimestamp,
		changeUUID,
		op,
		r.Resource.ObjectType,
		r.Resource.ObjectId,
		r.Relation,
		r.Subject.Object.ObjectType,
		r.Subject.Object.ObjectId,
		stringz.DefaultEmpty(r.Subject.OptionalRelation, datastore.Ellipsis),
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
