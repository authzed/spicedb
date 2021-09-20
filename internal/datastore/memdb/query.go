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

func iteratorForFilter(txn *memdb.Txn, filter *v1.RelationshipFilter) (memdb.ResultIterator, error) {
	switch {
	case filter.ResourceFilter.OptionalObjectId != "":
		return txn.Get(
			tableTuple,
			indexNamespaceAndObjectID,
			filter.ResourceFilter.ObjectType,
			filter.ResourceFilter.OptionalObjectId,
		)
	case filter.OptionalSubjectFilter != nil && filter.OptionalSubjectFilter.OptionalObjectId != "":
		return txn.Get(
			tableTuple,
			indexNamespaceAndUsersetID,
			filter.ResourceFilter.ObjectType,
			filter.OptionalSubjectFilter.ObjectType,
			filter.OptionalSubjectFilter.OptionalObjectId,
		)
	case filter.ResourceFilter.OptionalRelation != "":
		return txn.Get(
			tableTuple,
			indexNamespaceAndRelation,
			filter.ResourceFilter.ObjectType,
			filter.ResourceFilter.OptionalRelation,
		)
	}

	return txn.Get(
		tableTuple,
		indexNamespace,
		filter.ResourceFilter.ObjectType,
	)
}

func (mtq memdbTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	txn := mtq.db.Txn(false)

	time.Sleep(mtq.simulatedLatency)

	bestIterator, err := iteratorForFilter(txn, &v1.RelationshipFilter{
		ResourceFilter:        mtq.resourceFilter,
		OptionalSubjectFilter: mtq.usersetFilter,
	})
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

	return foundRaw.(*tupleEntry).RelationTuple()
}

func (mti *memdbTupleIterator) Err() error {
	return nil
}

func (mti *memdbTupleIterator) Close() {
	mti.txn.Abort()
	mti.txn = nil
}
