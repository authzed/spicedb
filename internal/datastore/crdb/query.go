package crdb

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/jackc/pgx/v4"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	querySetTransactionTime = "SET TRANSACTION AS OF SYSTEM TIME %s"
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

func (cds *crdbDatastore) QueryTuples(
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

	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	ctq := common.TupleQuerySplitter{
		Conn:                      cds.conn,
		PrepareTransaction:        cds.prepareTransaction,
		SplitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize,

		FilteredQueryBuilder: qBuilder,
		Revision:             revision,
		Limit:                queryOpts.Limit,
		Usersets:             queryOpts.Usersets,

		Tracer:    tracer,
		DebugName: "QueryTuples",
	}

	return ctq.SplitAndExecute(ctx)
}

func (cds *crdbDatastore) prepareTransaction(ctx context.Context, tx pgx.Tx, revision datastore.Revision) error {
	setTxTime := fmt.Sprintf(querySetTransactionTime, revision)
	_, err := tx.Exec(ctx, setTxTime)
	return err
}

func (cds *crdbDatastore) reverseQueryBase(qBuilder common.SchemaQueryFilterer, revision datastore.Revision) common.ReverseTupleQuery {
	return common.ReverseTupleQuery{
		TupleQuerySplitter: common.TupleQuerySplitter{
			Conn:                      cds.conn,
			PrepareTransaction:        cds.prepareTransaction,
			SplitAtEstimatedQuerySize: common.DefaultSplitAtEstimatedQuerySize,

			FilteredQueryBuilder: qBuilder,
			Revision:             revision,
			Limit:                nil,
			Usersets:             nil,

			Tracer:    tracer,
			DebugName: "ReverseQueryTuples",
		},
	}
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToSubjectFilter(tuple.UsersetToSubjectFilter(subject))

	return cds.reverseQueryBase(qBuilder, revision)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToSubjectFilter(&v1.SubjectFilter{
			SubjectType: subjectNamespace,
			OptionalRelation: &v1.SubjectFilter_RelationFilter{
				Relation: subjectRelation,
			},
		})

	return cds.reverseQueryBase(qBuilder, revision)
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectNamespace(subjectNamespace string, revision datastore.Revision) datastore.ReverseTupleQuery {
	qBuilder := common.NewSchemaQueryFilterer(schema, queryTuples).
		FilterToSubjectFilter(&v1.SubjectFilter{
			SubjectType: subjectNamespace,
		})

	return cds.reverseQueryBase(qBuilder, revision)
}
