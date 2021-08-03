package crdb

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

func (cds *crdbDatastore) ReverseQueryTuplesFromSubject(subject *v0.ObjectAndRelation, revision datastore.Revision) datastore.ReverseTupleQuery {
	return crdbReverseTupleQuery{
		commonTupleQuery: commonTupleQuery{
			conn: cds.conn,
			query: queryTuples.Where(sq.Eq{
				colUsersetNamespace: subject.Namespace,
				colUsersetRelation:  subject.Relation,
				colUsersetObjectID:  subject.ObjectId,
			}),
			revision:  revision,
			isReverse: true,
		},
	}
}

func (cds *crdbDatastore) ReverseQueryTuplesFromSubjectRelation(subjectNamespace, subjectRelation string, revision datastore.Revision) datastore.ReverseTupleQuery {
	return crdbReverseTupleQuery{
		commonTupleQuery: commonTupleQuery{
			conn: cds.conn,
			query: queryTuples.Where(sq.Eq{
				colUsersetNamespace: subjectNamespace,
				colUsersetRelation:  subjectRelation,
			}),
			revision:  revision,
			isReverse: true,
		},
	}
}

type crdbReverseTupleQuery struct {
	commonTupleQuery
}

func (crtq crdbReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	crtq.query = crtq.query.
		Where(sq.Eq{
			colNamespace: namespaceName,
			colRelation:  relationName,
		})
	return crtq
}
