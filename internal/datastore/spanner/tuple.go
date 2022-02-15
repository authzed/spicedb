package spanner

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/google/uuid"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"google.golang.org/api/iterator"

	"github.com/authzed/spicedb/internal/datastore"
)

func (sd spannerDatastore) WriteTuples(ctx context.Context, preconditions []*v1.Precondition, mutations []*v1.RelationshipUpdate) (datastore.Revision, error) {
	ts, err := sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		if err := checkPreconditions(ctx, rwt, preconditions); err != nil {
			return err
		}

		changeUUID := uuid.New().String()

		for _, mutation := range mutations {
			var txnMut *spanner.Mutation
			var op int
			switch mutation.Operation {
			case v1.RelationshipUpdate_OPERATION_TOUCH:
				txnMut = spanner.InsertOrUpdate(tableRelationship, allRelationshipCols, upsertVals(mutation.Relationship))
				op = colChangeOpTouch
			case v1.RelationshipUpdate_OPERATION_CREATE:
				txnMut = spanner.Insert(tableRelationship, allRelationshipCols, upsertVals(mutation.Relationship))
				op = colChangeOpCreate
			case v1.RelationshipUpdate_OPERATION_DELETE:
				txnMut = spanner.Delete(tableRelationship, keyFromRelationship(mutation.Relationship))
				op = colChangeOpDelete
			default:
				log.Ctx(ctx).Error().Stringer("operation", mutation.Operation).Msg("unknown operation type")
				return fmt.Errorf("unknown mutation operation: %s", mutation.Operation)
			}

			changelogMut := spanner.Insert(tableChangelog, allChangelogCols, changeVals(changeUUID, op, mutation.Relationship))
			if err := rwt.BufferWrite([]*spanner.Mutation{txnMut, changelogMut}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToWriteTuples, err)
	}

	return revisionFromTimestamp(ts), nil
}

func (sd spannerDatastore) DeleteRelationships(ctx context.Context, preconditions []*v1.Precondition, filter *v1.RelationshipFilter) (datastore.Revision, error) {
	ts, err := sd.client.ReadWriteTransaction(ctx, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		if err := checkPreconditions(ctx, rwt, preconditions); err != nil {
			return err
		}

		return deleteWithFilter(ctx, rwt, filter)
	})
	if err != nil {
		return datastore.NoRevision, fmt.Errorf(errUnableToDeleteTuples, err)
	}

	return revisionFromTimestamp(ts), nil
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
	selectDeleted := sql.Select().Column(fmt.Sprintf("%s, \"%s\", %d, %s, %s, %s, %s, %s, %s",
		funcPendingCommitTimestamp,
		uuid.New(),
		colChangeOpDelete,
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
	)).From(tableRelationship)

	queries := selectAndDelete{selectDeleted, sql.Delete(tableRelationship)}

	// Add clauses for the ResourceFilter
	queries = queries.Where(sq.Eq{colNamespace: filter.ResourceType})
	// tracerAttributes := []attribute.KeyValue{common.ObjNamespaceNameKey.String(filter.ResourceType)}
	if filter.OptionalResourceId != "" {
		queries = queries.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
		// tracerAttributes = append(tracerAttributes, common.ObjIDKey.String(filter.OptionalResourceId))
	}
	if filter.OptionalRelation != "" {
		queries = queries.Where(sq.Eq{colRelation: filter.OptionalRelation})
		// tracerAttributes = append(tracerAttributes, common.ObjRelationNameKey.String(filter.OptionalRelation))
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		queries = queries.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		// tracerAttributes = append(tracerAttributes, common.SubNamespaceNameKey.String(subjectFilter.SubjectType))
		if subjectFilter.OptionalSubjectId != "" {
			queries = queries.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
			// tracerAttributes = append(tracerAttributes, common.SubObjectIDKey.String(subjectFilter.OptionalSubjectId))
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			queries = queries.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
			// tracerAttributes = append(tracerAttributes, common.SubRelationNameKey.String(relationFilter.Relation))
		}
	}

	insertSel := queries.sel.Prefix("(").Suffix(")")
	insertQuery := sql.
		Insert(tableChangelog).
		Columns(
			colChangeTS,
			colChangeUUID,
			colChangeOp,
			colChangeNamespace,
			colChangeObjectID,
			colChangeRelation,
			colChangeUsersetNamespace,
			colChangeUsersetObjectID,
			colChangeUsersetRelation,
		).Select(insertSel)

	isql, iargs, err := insertQuery.ToSql()
	if err != nil {
		return err
	}

	_, err = rwt.Update(ctx, statementFromSQL(isql, iargs))
	if err != nil {
		return err
	}

	sql, args, err := queries.del.ToSql()
	if err != nil {
		return err
	}

	_, err = rwt.Update(ctx, statementFromSQL(sql, args))
	if err != nil {
		return err
	}

	return nil
}

func checkPreconditions(ctx context.Context, rwt *spanner.ReadWriteTransaction, preconditions []*v1.Precondition) error {
	for _, precond := range preconditions {
		f := precond.Filter

		query := queryTuples.Where(sq.Eq{colNamespace: f.ResourceType}).Limit(1)

		if f.OptionalResourceId != "" {
			query = query.Where(sq.Eq{colObjectID: f.OptionalResourceId})
		}

		if f.OptionalRelation != "" {
			query = query.Where(sq.Eq{colRelation: f.OptionalRelation})
		}

		if f.OptionalSubjectFilter != nil {
			subF := f.OptionalSubjectFilter

			query = query.Where(sq.Eq{colUsersetNamespace: subF.SubjectType})

			if subF.OptionalSubjectId != "" {
				query = query.Where(sq.Eq{colUsersetObjectID: subF.OptionalSubjectId})
			}

			if subF.OptionalRelation != nil {
				subRelation := stringz.DefaultEmpty(subF.OptionalRelation.Relation, datastore.Ellipsis)
				query = query.Where(sq.Eq{colUsersetRelation: subRelation})
			}
		}

		sql, args, err := query.ToSql()
		if err != nil {
			return err
		}

		iter := rwt.Query(ctx, statementFromSQL(sql, args))
		defer iter.Stop()

		first, err := iter.Next()
		if err != nil && !errors.Is(err, iterator.Done) {
			return err
		}

		if errors.Is(err, iterator.Done) && precond.Operation == v1.Precondition_OPERATION_MUST_MATCH {
			// Didn't find tuples when we expected to
			return datastore.NewPreconditionFailedErr(precond)
		}

		if err == nil && first != nil && precond.Operation == v1.Precondition_OPERATION_MUST_NOT_MATCH {
			// Found a tuple when we didn't expect to
			return datastore.NewPreconditionFailedErr(precond)
		}
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
