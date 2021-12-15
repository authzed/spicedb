package memdb

import (
	"context"
	"fmt"
	"runtime"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/options"
)

func (mds *memdbDatastore) ReverseQueryTuples(
	ctx context.Context,
	subjectFilter *v1.SubjectFilter,
	revision datastore.Revision,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.TupleIterator, error) {
	db := mds.db
	if db == nil {
		return nil, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	time.Sleep(mds.simulatedLatency)

	var err error
	var bestIterator memdb.ResultIterator
	if queryOpts.ResRelation != nil {
		bestIterator, err = txn.Get(
			tableRelationship,
			indexSubjectAndResourceRelation,
			subjectFilter.SubjectType,
			queryOpts.ResRelation.Namespace,
			queryOpts.ResRelation.Relation,
		)
	} else if subjectFilter.OptionalSubjectId != "" && subjectFilter.OptionalRelation != nil {
		bestIterator, err = txn.Get(
			tableRelationship,
			indexFullSubject,
			subjectFilter.SubjectType,
			subjectFilter.OptionalSubjectId,
			stringz.DefaultEmpty(subjectFilter.OptionalRelation.Relation, datastore.Ellipsis),
		)
	} else {
		bestIterator, err = txn.Get(
			tableRelationship,
			indexSubjectNamespace,
			subjectFilter.SubjectType,
		)
	}

	if err != nil {
		txn.Abort()
		return nil, fmt.Errorf(errUnableToQueryTuples, err)
	}

	filterObjectType, filterRelation := "", ""
	if queryOpts.ResRelation != nil {
		filterObjectType = queryOpts.ResRelation.Namespace
		filterRelation = queryOpts.ResRelation.Relation
	}

	matchingRelationshipsFilterFunc := filterFuncForFilters(
		filterObjectType,
		"",
		filterRelation,
		subjectFilter,
		nil,
	)
	filteredIterator := memdb.NewFilterIterator(bestIterator, matchingRelationshipsFilterFunc)
	filteredAlive := memdb.NewFilterIterator(filteredIterator, filterToLiveObjects(revision))

	iter := &memdbTupleIterator{
		txn:   txn,
		it:    filteredAlive,
		limit: queryOpts.ReverseLimit,
	}

	runtime.SetFinalizer(iter, func(iter *memdbTupleIterator) {
		if iter.txn != nil {
			panic("Tuple iterator garbage collected before Close() was called")
		}
	})

	return iter, nil
}
