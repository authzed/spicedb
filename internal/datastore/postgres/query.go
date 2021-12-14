package postgres

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

var queryTuples = psql.Select(
	colNamespace,
	colObjectID,
	colRelation,
	colUsersetNamespace,
	colUsersetObjectID,
	colUsersetRelation,
).From(tableTuple)

var schema = common.SchemaInformation{
	ColNamespace:        colNamespace,
	ColObjectID:         colObjectID,
	ColRelation:         colRelation,
	ColUsersetNamespace: colUsersetNamespace,
	ColUsersetObjectID:  colUsersetObjectID,
	ColUsersetRelation:  colUsersetRelation,
}

func (pgd *pgDatastore) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	opts ...options.QueryOptionsOption,
) (iter datastore.TupleIterator, err error) {
	qBuilder := common.NewSchemaQueryFilterer(schema, filterToLivingObjects(queryTuples, revision)).
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

	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	ctq := common.TupleQuerySplitter{
		Conn:                      pgd.dbpool,
		PrepareTransaction:        nil,
		SplitAtEstimatedQuerySize: pgd.splitAtEstimatedQuerySize,

		FilteredQueryBuilder: qBuilder,
		Revision:             revision,
		Limit:                queryOpts.Limit,
		Usersets:             queryOpts.Usersets,

		Tracer:    tracer,
		DebugName: "QueryTuples",
	}

	return ctq.SplitAndExecute(ctx)
}

func (pgd *pgDatastore) reverseQueryBase(qBuilder common.SchemaQueryFilterer, revision datastore.Revision) common.ReverseTupleQuery {
	return common.ReverseTupleQuery{
		TupleQuerySplitter: common.TupleQuerySplitter{
			Conn:                      pgd.dbpool,
			PrepareTransaction:        nil,
			SplitAtEstimatedQuerySize: pgd.splitAtEstimatedQuerySize,

			FilteredQueryBuilder: qBuilder,
			Revision:             revision,
			Limit:                nil,
			Usersets:             nil,

			Tracer:    tracer,
			DebugName: "ReverseQueryTuples",
		},
	}
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	qBuilder := common.NewSchemaQueryFilterer(schema, filterToLivingObjects(queryTuples, revision)).
		FilterToSubjectFilter(tuple.UsersetToSubjectFilter(subject))

	return pgd.reverseQueryBase(qBuilder, revision)
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	qBuilder := common.NewSchemaQueryFilterer(schema, filterToLivingObjects(queryTuples, revision)).
		FilterToSubjectFilter(&v1.SubjectFilter{
			SubjectType: subjectNamespace,
			OptionalRelation: &v1.SubjectFilter_RelationFilter{
				Relation: subjectRelation,
			},
		})

	return pgd.reverseQueryBase(qBuilder, revision)
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	qBuilder := common.NewSchemaQueryFilterer(schema, filterToLivingObjects(queryTuples, revision)).
		FilterToSubjectFilter(&v1.SubjectFilter{
			SubjectType: subjectNamespace,
		})

	return pgd.reverseQueryBase(qBuilder, revision)
}
