package crdb

import (
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

func (cds *crdbDatastore) ReverseQueryTuples(revision datastore.Revision) datastore.ReverseTupleQuery {
	return crdbReverseTupleQuery{
		commonTupleQuery: commonTupleQuery{
			conn:     cds.conn,
			query:    queryTuples,
			revision: revision,
		},
	}
}

type crdbReverseTupleQuery struct {
	commonTupleQuery

	subNamespaceName string
}

func (crtq crdbReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	crtq.query = crtq.query.
		Where(sq.Eq{
			colNamespace: namespaceName,
			colRelation:  relationName,
		})
	return crtq
}

func (crtq crdbReverseTupleQuery) WithSubjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	if crtq.subNamespaceName != "" {
		panic("WithSubject or WithSubjectRelation already called")
	}

	crtq.subNamespaceName = namespaceName

	crtq.query = crtq.query.Where(sq.Eq{
		colUsersetNamespace: namespaceName,
		colUsersetRelation:  relationName,
	})
	return crtq
}

func (crtq crdbReverseTupleQuery) WithSubject(onr *pb.ObjectAndRelation) datastore.ReverseTupleQuery {
	if crtq.subNamespaceName != "" {
		panic("WithSubject or WithSubjectRelation already called")
	}

	crtq.subNamespaceName = onr.Namespace

	crtq.query = crtq.query.Where(sq.Eq{
		colUsersetNamespace: onr.Namespace,
		colUsersetRelation:  onr.Relation,
		colUsersetObjectID:  onr.ObjectId,
	})
	return crtq
}
