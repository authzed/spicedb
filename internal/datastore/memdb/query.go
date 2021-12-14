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
	db := mds.db
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

	filteredIterator := memdb.NewFilterIterator(bestIterator, func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*relationship)

		switch {
		case filter.OptionalResourceId != "" && filter.OptionalResourceId != tuple.resourceID:
			return true
		case filter.OptionalRelation != "" && filter.OptionalRelation != tuple.relation:
			return true
		}

		if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
			switch {
			case subjectFilter.SubjectType != tuple.subjectNamespace:
				return true
			case subjectFilter.OptionalSubjectId != "" && subjectFilter.OptionalSubjectId != tuple.subjectObjectID:
				return true
			case subjectFilter.OptionalRelation != nil && stringz.DefaultEmpty(subjectFilter.OptionalRelation.Relation, datastore.Ellipsis) != tuple.subjectRelation:
				return true
			}
		}

		if len(queryOpts.Usersets) > 0 {
			found := false
			for _, filter := range queryOpts.Usersets {
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
	})

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

func (mti *memdbTupleIterator) Next() *v0.RelationTuple {
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
