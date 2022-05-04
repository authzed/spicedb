package spanner

import (
	"context"

	"cloud.google.com/go/spanner"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

var queryTuples = sql.Select(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
).From(tableRelationship)

var schema = common.SchemaInformation{
	ColNamespace:        colNamespace,
	ColObjectID:         colObjectID,
	ColRelation:         colRelation,
	ColUsersetNamespace: colUsersetNamespace,
	ColUsersetObjectID:  colUsersetObjectID,
	ColUsersetRelation:  colUsersetRelation,
}

func (sd spannerDatastore) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	opts ...options.QueryOptionsOption,
) (iter datastore.TupleIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToResourceType(filter.ResourceType)

	if filter.OptionalResourceId != "" {
		qBuilder = qBuilder.FilterToResourceID(filter.OptionalResourceId)
	}

	if filter.OptionalRelation != "" {
		qBuilder = qBuilder.FilterToRelation(filter.OptionalRelation)
	}

	if filter.OptionalSubjectFilter != nil {
		qBuilder = qBuilder.FilterToSubjectFilter(filter.OptionalSubjectFilter)
	}

	return sd.querySplitter.SplitAndExecuteQuery(ctx, qBuilder, revision, opts...)
}

func (sd spannerDatastore) ReverseQueryTuples(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	opts ...options.ReverseQueryOptionsOption,
) (iter datastore.TupleIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToSubjectFilter(subjectFilter)

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	if queryOpts.ResRelation != nil {
		qBuilder = qBuilder.
			FilterToResourceType(queryOpts.ResRelation.Namespace).
			FilterToRelation(queryOpts.ResRelation.Relation)
	}

	return sd.querySplitter.SplitAndExecuteQuery(ctx,
		qBuilder,
		revision,
		options.WithLimit(queryOpts.ReverseLimit),
	)
}

func queryExecutor(client *spanner.Client) common.ExecuteQueryFunc {
	return func(
		ctx context.Context,
		revision datastore.Revision,
		sql string,
		args []interface{},
	) ([]*core.RelationTuple, error) {
		ctx, span := tracer.Start(ctx, "ExecuteQuery")
		defer span.End()
		iter := client.
			Single().
			WithTimestampBound(spanner.ReadTimestamp(timestampFromRevision(revision))).
			Query(ctx, statementFromSQL(sql, args))

		var tuples []*core.RelationTuple

		if err := iter.Do(func(row *spanner.Row) error {
			nextTuple := &core.RelationTuple{
				ObjectAndRelation: &core.ObjectAndRelation{},
				User: &core.User{
					UserOneof: &core.User_Userset{
						Userset: &core.ObjectAndRelation{},
					},
				},
			}
			userset := nextTuple.User.GetUserset()
			err := row.Columns(
				&nextTuple.ObjectAndRelation.Namespace,
				&nextTuple.ObjectAndRelation.ObjectId,
				&nextTuple.ObjectAndRelation.Relation,
				&userset.Namespace,
				&userset.ObjectId,
				&userset.Relation,
			)
			if err != nil {
				return err
			}

			tuples = append(tuples, nextTuple)

			return nil
		}); err != nil {
			return nil, err
		}

		return tuples, nil
	}
}
