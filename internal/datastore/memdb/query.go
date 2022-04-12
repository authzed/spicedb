package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
)

func iteratorForFilter(txn *memdb.Txn, filter *v1.RelationshipFilter) (memdb.ResultIterator, error) {
	switch {
	case filter.OptionalResourceId != "":
		return txn.Get(
			tableRelationship,
			indexNamespaceAndResourceID,
			filter.ResourceType,
			filter.OptionalResourceId,
		)
	case filter.OptionalSubjectFilter != nil && filter.OptionalSubjectFilter.OptionalSubjectId != "":
		return txn.Get(
			tableRelationship,
			indexNamespaceAndSubjectID,
			filter.ResourceType,
			filter.OptionalSubjectFilter.SubjectType,
			filter.OptionalSubjectFilter.OptionalSubjectId,
		)
	case filter.OptionalRelation != "":
		return txn.Get(
			tableRelationship,
			indexNamespaceAndRelation,
			filter.ResourceType,
			filter.OptionalRelation,
		)
	}

	return txn.Get(tableRelationship, indexNamespace, filter.ResourceType)
}

func (mds *memdbDatastore) QueryTuples(
	ctx context.Context,
	filter *v1.RelationshipFilter,
	revision datastore.Revision,
	opts ...options.QueryOptionsOption,
) (datastore.TupleIterator, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("memdb closed")
	}

	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	txn := db.Txn(false)

	time.Sleep(mds.simulatedLatency)

	bestIterator, err := iteratorForFilter(txn, filter)
	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	matchingRelationshipsFilterFunc := filterFuncForFilters(
		filter.ResourceType,
		filter.OptionalResourceId,
		filter.OptionalRelation,
		filter.OptionalSubjectFilter,
		queryOpts.Usersets,
	)
	filteredIterator := memdb.NewFilterIterator(bestIterator, matchingRelationshipsFilterFunc)
	filteredAlive := memdb.NewFilterIterator(filteredIterator, filterToLiveObjects(revision))

	iter := &memdbTupleIterator{
		txn:   txn,
		it:    filteredAlive,
		limit: queryOpts.Limit,
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

func filterFuncForFilters(optionalObjectType, optionalObjectID, optionalRelation string,
	optionalSubjectFilter *v1.SubjectFilter, usersets []*core.ObjectAndRelation,
) memdb.FilterFunc {
	return func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*relationship)

		switch {
		case optionalObjectType != "" && optionalObjectType != tuple.namespace:
			return true
		case optionalObjectID != "" && optionalObjectID != tuple.resourceID:
			return true
		case optionalRelation != "" && optionalRelation != tuple.relation:
			return true
		}

		if optionalSubjectFilter != nil {
			switch {
			case optionalSubjectFilter.SubjectType != tuple.subjectNamespace:
				return true
			case optionalSubjectFilter.OptionalSubjectId != "" && optionalSubjectFilter.OptionalSubjectId != tuple.subjectObjectID:
				return true
			case optionalSubjectFilter.OptionalRelation != nil && stringz.DefaultEmpty(optionalSubjectFilter.OptionalRelation.Relation, datastore.Ellipsis) != tuple.subjectRelation:
				return true
			}
		}

		if len(usersets) > 0 {
			found := false
			for _, filter := range usersets {
				if filter.Namespace == tuple.subjectNamespace &&
					filter.ObjectId == tuple.subjectObjectID &&
					filter.Relation == tuple.subjectRelation {
					found = true
					break
				}
			}
			return !found
		}

		return false
	}
}

func (mti *memdbTupleIterator) Next() *core.RelationTuple {
	foundRaw := mti.it.Next()
	if foundRaw == nil {
		return nil
	}

	if mti.limit != nil && mti.count >= *mti.limit {
		return nil
	}
	mti.count++

	return foundRaw.(*relationship).RelationTuple()
}

func (mti *memdbTupleIterator) Err() error {
	return nil
}

func (mti *memdbTupleIterator) Close() {
	mti.txn.Abort()
	mti.txn = nil
}
