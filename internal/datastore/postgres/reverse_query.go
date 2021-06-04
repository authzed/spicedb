package postgres

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel/attribute"
)

func (pgd *pgDatastore) ReverseQueryTuplesFromSubject(subject *pb.ObjectAndRelation, revision decimal.Decimal) datastore.ReverseTupleQuery {
	baseQuery := pgd.newBaseQuery(subject.Namespace, subject.Relation, revision)
	baseQuery.query = baseQuery.query.Where(sq.Eq{colUsersetObjectID: subject.ObjectId})
	baseQuery.tracerAttributes = append(baseQuery.tracerAttributes, subObjectIDKey.String(subject.ObjectId))
	return baseQuery
}

func (pgd *pgDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision decimal.Decimal) datastore.ReverseTupleQuery {
	return pgd.newBaseQuery(subjectNamespace, subjectRelation, revision)
}

func (pgd *pgDatastore) newBaseQuery(subjectNamespace, subjectRelation string, revision decimal.Decimal) pgReverseTupleQuery {
	return pgReverseTupleQuery{
		commonTupleQuery: commonTupleQuery{
			db: pgd.db,
			query: queryTuples.
				Where(sq.LtOrEq{colCreatedTxn: transactionFromRevision(revision)}).
				Where(sq.Or{
					sq.Eq{colDeletedTxn: liveDeletedTxnID},
					sq.Gt{colDeletedTxn: transactionFromRevision(revision)},
				}).
				Where(sq.Eq{
					colUsersetNamespace: subjectNamespace,
					colUsersetRelation:  subjectRelation,
				}),
			tracerAttributes: []attribute.KeyValue{
				subNamespaceKey.String(subjectNamespace),
				subRelationKey.String(subjectRelation),
			},
		},
	}

}

type pgReverseTupleQuery struct {
	commonTupleQuery
}

var (
	objNamespaceNameKey = attribute.Key("authzed.com/spicedb/objNamespaceName")
	objRelationNameKey  = attribute.Key("authzed.com/spicedb/objRelationName")

	subNamespaceKey = attribute.Key("authzed.com/spicedb/subNamespaceName")
	subRelationKey  = attribute.Key("authzed.com/spicedb/subRelationName")
	subObjectIDKey  = attribute.Key("authzed.com/spicedb/subObjectId")
)

func (ptq pgReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	ptq.tracerAttributes = append(ptq.tracerAttributes, objNamespaceNameKey.String(namespaceName))
	ptq.tracerAttributes = append(ptq.tracerAttributes, objRelationNameKey.String(relationName))
	ptq.query = ptq.query.
		Where(sq.Eq{
			colNamespace: namespaceName,
			colRelation:  relationName,
		})
	return ptq
}
