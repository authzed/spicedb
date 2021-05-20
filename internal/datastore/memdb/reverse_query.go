package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type memdbReverseTupleQuery struct {
	db       *memdb.MemDB
	revision datastore.Revision

	objNamespaceName string
	objRelationName  string

	subNamespaceName string
	subRelationName  string
	subObjectId      string

	limit *uint64

	simulatedLatency time.Duration
}

func (mtq memdbReverseTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	mtq.limit = &limit
	return mtq
}

func (mtq memdbReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	mtq.objNamespaceName = namespaceName
	mtq.objRelationName = relationName
	return mtq
}

func (mtq memdbReverseTupleQuery) WithSubjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	mtq.subNamespaceName = namespaceName
	mtq.subRelationName = relationName
	return mtq
}

func (mtq memdbReverseTupleQuery) WithSubject(onr *pb.ObjectAndRelation) datastore.ReverseTupleQuery {
	mtq.subNamespaceName = onr.Namespace
	mtq.subRelationName = onr.Relation
	mtq.subObjectId = onr.ObjectId
	return mtq
}

func (mtq memdbReverseTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	txn := mtq.db.Txn(false)

	time.Sleep(mtq.simulatedLatency)

	var err error
	var bestIterator memdb.ResultIterator
	if mtq.objNamespaceName != "" {
		if mtq.subNamespaceName == "" || mtq.subRelationName == "" {
			return nil, fmt.Errorf("missing subject namespace or relation for object")
		}

		if mtq.subObjectId != "" {
			bestIterator, err = txn.Get(
				tableTuple,
				indexRelationAndUserset,
				mtq.subNamespaceName,
				mtq.subObjectId,
				mtq.subRelationName,
				mtq.objNamespaceName,
				mtq.objRelationName,
			)
		} else {
			bestIterator, err = txn.Get(
				tableTuple,
				indexRelationAndRelation,
				mtq.subNamespaceName,
				mtq.subRelationName,
				mtq.objNamespaceName,
				mtq.objRelationName,
			)
		}
	} else if mtq.subObjectId != "" {
		bestIterator, err = txn.Get(
			tableTuple,
			indexUserset,
			mtq.subNamespaceName,
			mtq.subObjectId,
			mtq.subRelationName,
		)
	} else {
		bestIterator, err = txn.Get(
			tableTuple,
			indexUsersetRelation,
			mtq.subNamespaceName,
			mtq.subRelationName,
		)
	}

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)
		if uint64(mtq.revision.IntPart()) < tuple.createdTxn || uint64(mtq.revision.IntPart()) >= tuple.deletedTxn {
			return true
		}
		return false
	})

	iter := &memdbTupleIterator{
		txn:   txn,
		it:    filteredIterator,
		limit: mtq.limit,
	}

	runtime.SetFinalizer(iter, func(iter *memdbTupleIterator) {
		if iter.txn != nil {
			panic("Tuple iterator garbage collected before Close() was called")
		}
	})

	return iter, nil
}
