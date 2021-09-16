package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
)

type memdbTupleQuery struct {
	db       *memdb.MemDB
	revision datastore.Revision

	resourceFilter *v1.ObjectFilter
	usersetFilter  *v1.ObjectFilter
	usersetsFilter []*v0.ObjectAndRelation
	limit          *uint64

	simulatedLatency time.Duration
}

func (mtq memdbTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	mtq.limit = &limit
	return mtq
}

func (mtq memdbTupleQuery) WithUsersetFilter(filter *v1.ObjectFilter) datastore.TupleQuery {
	if filter == nil {
		panic("cannot call WithUsersetFilter with a nil filter")
	}

	if mtq.usersetFilter != nil {
		panic("cannot call WithUsersetFilter after WithUsersets")
	}

	if mtq.usersetsFilter != nil {
		panic("called WithUsersetFilter twice")
	}

	mtq.usersetFilter = filter
	return mtq
}

func (mtq memdbTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if mtq.usersetFilter != nil {
		panic("cannot call WithUsersets after WithUsersetFilter")
	}

	if mtq.usersetsFilter != nil {
		panic("called WithUsersets twice")
	}

	if len(usersets) == 0 {
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
	if mtq.resourceFilter.OptionalObjectId != "" {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndObjectID,
			mtq.resourceFilter.ObjectType,
			mtq.resourceFilter.OptionalObjectId,
		)
	} else if mtq.usersetFilter != nil && mtq.usersetFilter.OptionalObjectId != "" {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndUsersetID,
			mtq.resourceFilter.ObjectType,
			mtq.usersetFilter.ObjectType,
			mtq.usersetFilter.OptionalObjectId,
		)
	} else if mtq.usersetsFilter != nil && len(mtq.usersetsFilter) > 0 {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespace,
			mtq.resourceFilter.ObjectType,
		)
	} else if mtq.resourceFilter.OptionalRelation != "" {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespaceAndRelation,
			mtq.resourceFilter.ObjectType,
			mtq.resourceFilter.OptionalRelation,
		)
	} else {
		bestIterator, err = txn.Get(
			tableTuple,
			indexNamespace,
			mtq.resourceFilter.ObjectType,
		)
	}

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)

		if mtq.resourceFilter.OptionalObjectId != "" && mtq.resourceFilter.OptionalObjectId != tuple.objectID {
			return true
		}
		if mtq.resourceFilter.OptionalRelation != "" && mtq.resourceFilter.OptionalRelation != tuple.relation {
			return true
		}
		if mtq.usersetFilter != nil {
			if mtq.usersetFilter.ObjectType != tuple.usersetNamespace {
				return true
			}
			if mtq.usersetFilter.OptionalObjectId != "" &&
				mtq.usersetFilter.OptionalObjectId != tuple.usersetObjectID {
				return true
			}
			if mtq.usersetFilter.OptionalRelation != "" &&
				mtq.usersetFilter.OptionalRelation != tuple.usersetRelation {
				return true
			}
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
