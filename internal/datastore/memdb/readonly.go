package memdb

import (
	"context"
	"fmt"
	"runtime"

	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"

	"github.com/authzed/spicedb/internal/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type txFactory func() (*memdb.Txn, error)

type memdbReader struct {
	TryLocker
	txSource txFactory
	revision datastore.Revision
	initErr  error
}

// QueryRelationships reads relationships starting from the resource side.
func (r *memdbReader) QueryRelationships(
	ctx context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	queryOpts := options.NewQueryOptionsWithOptions(opts...)

	bestIterator, err := iteratorForFilter(tx, filter)
	if err != nil {
		return nil, err
	}

	matchingRelationshipsFilterFunc := filterFuncForFilters(
		filter.ResourceType,
		filter.OptionalResourceIds,
		filter.OptionalResourceRelation,
		filter.OptionalSubjectsFilter,
		queryOpts.Usersets,
	)
	filteredIterator := memdb.NewFilterIterator(bestIterator, matchingRelationshipsFilterFunc)

	iter := &memdbTupleIterator{
		it:    filteredIterator,
		limit: queryOpts.Limit,
	}

	runtime.SetFinalizer(iter, func(iter *memdbTupleIterator) {
		if !iter.closed {
			panic("Tuple iterator garbage collected before Close() was called")
		}
	})

	return iter, nil
}

// ReverseQueryRelationships reads relationships starting from the subject.
func (r *memdbReader) ReverseQueryRelationships(
	ctx context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	queryOpts := options.NewReverseQueryOptionsWithOptions(opts...)

	iterator, err := tx.Get(
		tableRelationship,
		indexSubjectNamespace,
		subjectsFilter.SubjectType,
	)
	if err != nil {
		return nil, err
	}

	filterObjectType, filterRelation := "", ""
	if queryOpts.ResRelation != nil {
		filterObjectType = queryOpts.ResRelation.Namespace
		filterRelation = queryOpts.ResRelation.Relation
	}

	matchingRelationshipsFilterFunc := filterFuncForFilters(
		filterObjectType,
		nil,
		filterRelation,
		&subjectsFilter,
		nil,
	)
	filteredIterator := memdb.NewFilterIterator(iterator, matchingRelationshipsFilterFunc)

	iter := &memdbTupleIterator{
		it:    filteredIterator,
		limit: queryOpts.ReverseLimit,
	}

	runtime.SetFinalizer(iter, func(iter *memdbTupleIterator) {
		if !iter.closed {
			panic("Tuple iterator garbage collected before Close() was called")
		}
	})

	return iter, nil
}

// ReadNamespace reads a namespace definition and version and returns it, and the revision at
// which it was created or last written, if found.
func (r *memdbReader) ReadNamespace(ctx context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	if r.initErr != nil {
		return nil, datastore.NoRevision, r.initErr
	}

	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	foundRaw, err := tx.First(tableNamespace, indexID, nsName)
	if err != nil {
		return nil, datastore.NoRevision, err
	}

	if foundRaw == nil {
		return nil, datastore.NoRevision, datastore.NewNamespaceNotFoundErr(nsName)
	}

	found := foundRaw.(*namespace)

	loaded := &core.NamespaceDefinition{}
	if err := loaded.UnmarshalVT(found.configBytes); err != nil {
		return nil, datastore.NoRevision, err
	}

	return loaded, found.updated, nil
}

// ListNamespaces lists all namespaces defined.
func (r *memdbReader) ListNamespaces(ctx context.Context) ([]*core.NamespaceDefinition, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	r.lockOrPanic()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	var nsDefs []*core.NamespaceDefinition

	it, err := tx.LowerBound(tableNamespace, indexID)
	if err != nil {
		return nil, err
	}

	for foundRaw := it.Next(); foundRaw != nil; foundRaw = it.Next() {
		found := foundRaw.(*namespace)

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(found.configBytes); err != nil {
			return nil, err
		}

		nsDefs = append(nsDefs, loaded)
	}

	return nsDefs, nil
}

func (r *memdbReader) lockOrPanic() {
	if !r.TryLock() {
		panic("detected concurrent use of ReadWriteTransaction")
	}
}

func iteratorForFilter(txn *memdb.Txn, filter datastore.RelationshipsFilter) (memdb.ResultIterator, error) {
	index := indexNamespace
	args := []any{filter.ResourceType}
	if filter.OptionalResourceRelation != "" {
		args = append(args, filter.OptionalResourceRelation)
		index = indexNamespaceAndRelation
	}

	iter, err := txn.Get(tableRelationship, index, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to get iterator for filter: %w", err)
	}

	return iter, err
}

func filterFuncForFilters(
	optionalResourceType string,
	optionalResourceIds []string,
	optionalRelation string,
	optionalSubjectsFilter *datastore.SubjectsFilter,
	usersets []*core.ObjectAndRelation,
) memdb.FilterFunc {
	return func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*relationship)

		switch {
		case optionalResourceType != "" && optionalResourceType != tuple.namespace:
			return true
		case len(optionalResourceIds) > 0 && !stringz.SliceContains(optionalResourceIds, tuple.resourceID):
			return true
		case optionalRelation != "" && optionalRelation != tuple.relation:
			return true
		}

		if optionalSubjectsFilter != nil {
			relations := make([]string, 0, 2)
			if optionalSubjectsFilter.RelationFilter.IncludeEllipsisRelation {
				relations = append(relations, datastore.Ellipsis)
			}

			if optionalSubjectsFilter.RelationFilter.NonEllipsisRelation != "" {
				relations = append(relations, optionalSubjectsFilter.RelationFilter.NonEllipsisRelation)
			}

			switch {
			case optionalSubjectsFilter.SubjectType != tuple.subjectNamespace:
				return true
			case len(optionalSubjectsFilter.OptionalSubjectIds) > 0 && !stringz.SliceContains(optionalSubjectsFilter.OptionalSubjectIds, tuple.subjectObjectID):
				return true
			case len(relations) > 0 && !stringz.SliceContains(relations, tuple.subjectRelation):
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

type memdbTupleIterator struct {
	closed bool
	it     memdb.ResultIterator
	limit  *uint64
	count  uint64
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
	mti.closed = true
}

var _ datastore.Reader = &memdbReader{}

type TryLocker interface {
	TryLock() bool
	Unlock()
}

type noopTryLocker struct{}

func (ntl noopTryLocker) TryLock() bool {
	return true
}

func (ntl noopTryLocker) Unlock() {}

var _ TryLocker = noopTryLocker{}
