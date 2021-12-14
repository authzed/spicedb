package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
)

type memdbReverseTupleQuery struct {
	db       *memdb.MemDB
	revision datastore.Revision

	objNamespaceName string
	objRelationName  string

	subNamespaceName string
	subRelationName  string
	subObjectID      string

	limit *uint64

	simulatedLatency time.Duration
}

func (mtq memdbReverseTupleQuery) Limit(limit uint64) datastore.ReverseTupleQuery {
	mtq.limit = &limit
	return mtq
}

func (mtq memdbReverseTupleQuery) WithObjectRelation(namespaceName string, relationName string) datastore.ReverseTupleQuery {
	mtq.objNamespaceName = namespaceName
	mtq.objRelationName = relationName
	return mtq
}

func (mtq memdbReverseTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	db := mtq.db
	if db == nil {
		return nil, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)

	time.Sleep(mtq.simulatedLatency)

	var err error
	var bestIterator memdb.ResultIterator
	if mtq.objNamespaceName != "" {
		if mtq.subObjectID != "" {
			bestIterator, err = txn.Get(
				tableRelationship,
				indexRelationAndSubject,
				mtq.subNamespaceName,
				mtq.subObjectID,
				mtq.subRelationName,
				mtq.objNamespaceName,
				mtq.objRelationName,
			)
		} else {
			bestIterator, err = txn.Get(
				tableRelationship,
				indexRelationAndRelation,
				mtq.subNamespaceName,
				mtq.subRelationName,
				mtq.objNamespaceName,
				mtq.objRelationName,
			)
		}
	} else if mtq.subObjectID != "" {
		bestIterator, err = txn.Get(
			tableRelationship,
			indexSubject,
			mtq.subNamespaceName,
			mtq.subObjectID,
			mtq.subRelationName,
		)
	} else if mtq.subRelationName != "" {
		bestIterator, err = txn.Get(
			tableRelationship,
			indexSubjectRelation,
			mtq.subNamespaceName,
			mtq.subRelationName,
		)
	} else {
		bestIterator, err = txn.Get(
			tableRelationship,
			indexSubjectNamespace,
			mtq.subNamespaceName,
		)
	}

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, filterToLiveObjects(mtq.revision))

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
