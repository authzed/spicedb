package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jzelinskie/stringz"
	"google.golang.org/protobuf/proto"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type spannerReadWriteTXN struct {
	spannerReader
	spannerRWT   *spanner.ReadWriteTransaction
	disableStats bool
}

func (rwt spannerReadWriteTXN) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	var rowCountChange int64
	for _, mutation := range mutations {
		txnMut, countChange, err := spannerMutation(ctx, mutation.Operation, mutation.Tuple)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
		rowCountChange += countChange

		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{txnMut}); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	if !rwt.disableStats {
		if err := updateCounter(ctx, rwt.spannerRWT, rowCountChange); err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func spannerMutation(
	ctx context.Context,
	operation core.RelationTupleUpdate_Operation,
	tpl *core.RelationTuple,
) (txnMut *spanner.Mutation, countChange int64, err error) {
	switch operation {
	case core.RelationTupleUpdate_TOUCH:
		countChange = 1
		txnMut = spanner.InsertOrUpdate(tableRelationship, allRelationshipCols, upsertVals(tpl))
	case core.RelationTupleUpdate_CREATE:
		countChange = 1
		txnMut = spanner.Insert(tableRelationship, allRelationshipCols, upsertVals(tpl))
	case core.RelationTupleUpdate_DELETE:
		countChange = -1
		txnMut = spanner.Delete(tableRelationship, keyFromRelationship(tpl))
	default:
		log.Ctx(ctx).Error().Stringer("operation", operation).Msg("unknown operation type")
		err = fmt.Errorf("unknown mutation operation: %s", operation)
		return
	}

	return
}

func (rwt spannerReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	if err := deleteWithFilter(ctx, rwt.spannerRWT, filter, rwt.disableStats); err != nil {
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

func deleteWithFilter(ctx context.Context, rwt *spanner.ReadWriteTransaction, filter *v1.RelationshipFilter, disableStats bool) error {
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

	sql, args, err := queries.del.ToSql()
	if err != nil {
		return err
	}

	numDeleted, err := rwt.Update(ctx, statementFromSQL(sql, args))
	if err != nil {
		return err
	}

	if !disableStats {
		if err := updateCounter(ctx, rwt, -1*numDeleted); err != nil {
			return err
		}
	}

	return nil
}

func upsertVals(r *core.RelationTuple) []any {
	key := keyFromRelationship(r)
	key = append(key, spanner.CommitTimestamp)
	key = append(key, caveatVals(r)...)
	return key
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

func caveatVals(r *core.RelationTuple) []any {
	if r.Caveat == nil {
		return []any{"", nil}
	}
	vals := []any{r.Caveat.CaveatName}
	if r.Caveat.Context != nil {
		vals = append(vals, spanner.NullJSON{Value: r.Caveat.Context, Valid: true})
	} else {
		vals = append(vals, nil)
	}
	return vals
}

func (rwt spannerReadWriteTXN) WriteNamespaces(_ context.Context, newConfigs ...*core.NamespaceDefinition) error {
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

func (rwt spannerReadWriteTXN) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	for _, nsName := range nsNames {
		relFilter := &v1.RelationshipFilter{ResourceType: nsName}
		if err := deleteWithFilter(ctx, rwt.spannerRWT, relFilter, rwt.disableStats); err != nil {
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
	var tpl *core.RelationTuple
	var err error
	for tpl, err = iter.Next(ctx); err == nil && tpl != nil; tpl, err = iter.Next(ctx) {
		txnMut, _, err := spannerMutation(ctx, core.RelationTupleUpdate_CREATE, tpl)
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

	if !rwt.disableStats {
		if err := updateCounter(ctx, rwt.spannerRWT, int64(numLoaded)); err != nil {
			return 0, fmt.Errorf(errUnableToBulkLoadRelationships, err)
		}
	}

	return numLoaded, nil
}

var _ datastore.ReadWriteTransaction = spannerReadWriteTXN{}
