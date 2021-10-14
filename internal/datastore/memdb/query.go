package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/datastore"
)

type memdbTupleQuery struct {
	db       *memdb.MemDB
	revision datastore.Revision

	resourceFilter        *v1.RelationshipFilter
	optionalSubjectFilter *v1.SubjectFilter
	usersetsFilter        []*v0.ObjectAndRelation
	limit                 *uint64

	simulatedLatency time.Duration
}

func (mtq memdbTupleQuery) Limit(limit uint64) datastore.CommonTupleQuery {
	mtq.limit = &limit
	return mtq
}

func (mtq memdbTupleQuery) WithSubjectFilter(filter *v1.SubjectFilter) datastore.TupleQuery {
	if filter == nil {
		panic("cannot call WithSubjectFilter with a nil filter")
	}

	if mtq.optionalSubjectFilter != nil {
		panic("cannot call WithSubjectFilter after WithUsersets")
	}

	if mtq.usersetsFilter != nil {
		panic("called WithSubjectFilter twice")
	}

	mtq.optionalSubjectFilter = filter
	return mtq
}

func (mtq memdbTupleQuery) WithUsersets(usersets []*v0.ObjectAndRelation) datastore.TupleQuery {
	if mtq.optionalSubjectFilter != nil {
		panic("cannot call WithUsersets after WithSubjectFilter")
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
	case filter.OptionalResourceId != "":
		return txn.Get(
			tableTuple,
			indexNamespaceAndObjectID,
			filter.ResourceType,
			filter.OptionalResourceId,
		)
	case filter.OptionalSubjectFilter != nil && filter.OptionalSubjectFilter.OptionalSubjectId != "":
		return txn.Get(
			tableTuple,
			indexNamespaceAndUsersetID,
			filter.ResourceType,
			filter.OptionalSubjectFilter.SubjectType,
			filter.OptionalSubjectFilter.OptionalSubjectId,
		)
	case filter.OptionalRelation != "":
		return txn.Get(
			tableTuple,
			indexNamespaceAndRelation,
			filter.ResourceType,
			filter.OptionalRelation,
		)
	}

	return txn.Get(tableTuple, indexNamespace, filter.ResourceType)
}

func (mtq memdbTupleQuery) Execute(ctx context.Context) (datastore.TupleIterator, error) {
	db := mtq.db
	if db == nil {
		return nil, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)

	time.Sleep(mtq.simulatedLatency)

	relationshipFilter := &v1.RelationshipFilter{
		ResourceType:          mtq.resourceFilter.ResourceType,
		OptionalResourceId:    mtq.resourceFilter.OptionalResourceId,
		OptionalRelation:      mtq.resourceFilter.OptionalRelation,
		OptionalSubjectFilter: mtq.optionalSubjectFilter,
	}

	bestIterator, err := iteratorForFilter(txn, relationshipFilter)
	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*tupleEntry)
		filter := relationshipFilter

		switch {
		case filter.OptionalResourceId != "" && filter.OptionalResourceId != tuple.objectID:
			return true
		case filter.OptionalRelation != "" && filter.OptionalRelation != tuple.relation:
			return true
		}

		if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
			switch {
			case subjectFilter.SubjectType != tuple.usersetNamespace:
				return true
			case subjectFilter.OptionalSubjectId != "" && subjectFilter.OptionalSubjectId != tuple.usersetObjectID:
				return true
			case subjectFilter.OptionalRelation != nil && stringz.DefaultEmpty(subjectFilter.OptionalRelation.Relation, datastore.Ellipsis) != tuple.usersetRelation:
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
	mti.count++

	return foundRaw.(*tupleEntry).RelationTuple()
}

func (mti *memdbTupleIterator) Err() error {
	return nil
}

func (mti *memdbTupleIterator) Close() {
	mti.txn.Abort()
	mti.txn = nil
}
