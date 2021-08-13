package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

type memdbTupleQuery struct {
	db        *memdb.MemDB
	namespace string
	revision  datastore.Revision

	objectIDFilter *string
	relationFilter *string
	usersetFilter  *v0.ObjectAndRelation
	usersetsFilter []*v0.ObjectAndRelation
	limit          *uint64

	simulatedLatency time.Duration
}

func (mtq memdbTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	mtq.limit = &limit
	return mtq
}

func (mtq memdbTupleQuery) WithObjectID(objectID string) datastore.TupleQuery {
	mtq.objectIDFilter = &objectID
	return mtq
}

func (mtq memdbTupleQuery) WithRelation(relation string) datastore.TupleQuery {
	mtq.relationFilter = &relation
	return mtq
}

func (mtq memdbTupleQuery) WithUserset(userset *v0.ObjectAndRelation) datastore.TupleQuery {
	mtq.usersetFilter = userset
	return mtq
}

func (mtq memdbTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if usersets == nil || len(usersets) == 0 {
		panic("Given nil or empty usersets")
	}
	mtq.usersetsFilter = usersets
	return mtq
}

func (mtq memdbTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	txn := mtq.db.Txn(false)

	time.Sleep(mtq.simulatedLatency)
	var err error
	var bestIterator memdb.ResultIterator
	if mtq.objectIDFilter != nil {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndObjectID,
			mtq.namespace,
			*mtq.objectIDFilter,
		)
	} else if mtq.usersetFilter != nil {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndUserset,
			mtq.namespace,
			mtq.usersetFilter.Namespace,
			mtq.usersetFilter.ObjectId,
			mtq.usersetFilter.Relation,
		)
	} else if mtq.usersetsFilter != nil && len(mtq.usersetsFilter) > 0 {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespace,
			mtq.namespace,
		)
	} else if mtq.relationFilter != nil {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndRelation,
			mtq.namespace,
			*mtq.relationFilter,
		)
	} else {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespace,
			mtq.namespace,
		)
	}

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)

		if mtq.objectIDFilter != nil && *mtq.objectIDFilter != tuple.objectID {
			return true
		}
		if mtq.relationFilter != nil && *mtq.relationFilter != tuple.relation {
			return true
		}
		if mtq.usersetFilter != nil && (mtq.usersetFilter.Namespace != tuple.usersetNamespace ||
			mtq.usersetFilter.ObjectId != tuple.usersetObjectID ||
			mtq.usersetFilter.Relation != tuple.usersetRelation) {
			return true
		}

		if len(mtq.usersetsFilter) > 0 {
			found := false
			for _, filter := range mtq.usersetsFilter {
				if filter.Namespace == tuple.usersetNamespace &&
					filter.ObjectId == tuple.usersetObjectID &&
					filter.Relation == tuple.usersetRelation {
					found = true
					break
				}
			}
			return !found
		}

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

type memdbTupleIterator struct {
	txn   *memdb.Txn
	it    memdb.ResultIterator
	limit *uint64
	count uint64
}

func (mti *memdbTupleIterator) Next() *v0.RelationTuple {
	foundRaw := mti.it.Next()
	if foundRaw == nil {
		return nil
	}

	if mti.limit != nil && mti.count >= *mti.limit {
		return nil
	}
	mti.count += 1

	found := foundRaw.(*tupleEntry)

	return &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: found.namespace,
			ObjectId:  found.objectID,
			Relation:  found.relation,
		},
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: &v0.ObjectAndRelation{
					Namespace: found.usersetNamespace,
					ObjectId:  found.usersetObjectID,
					Relation:  found.usersetRelation,
				},
			},
		},
	}
}

func (mti *memdbTupleIterator) Err() error {
	return nil
}

func (mti *memdbTupleIterator) Close() {
	mti.txn.Abort()
	mti.txn = nil
}
