package memdb

import (
	"context"
	"fmt"
	"runtime"
	"slices"
	"sort"

	"github.com/authzed/spicedb/pkg/spiceerrors"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type txFactory func() (*memdb.Txn, error)

type memdbReader struct {
	TryLocker
	txSource txFactory
	initErr  error
}

// QueryRelationships reads relationships starting from the resource side.
func (r *memdbReader) QueryRelationships(
	_ context.Context,
	filter datastore.RelationshipsFilter,
	opts ...options.QueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	r.mustLock()
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

	if queryOpts.After != nil && queryOpts.Sort == options.Unsorted {
		return nil, datastore.ErrCursorsWithoutSorting
	}

	matchingRelationshipsFilterFunc := filterFuncForFilters(
		filter.ResourceType,
		filter.OptionalResourceIds,
		filter.OptionalResourceRelation,
		filter.OptionalSubjectsSelectors,
		filter.OptionalCaveatName,
		makeCursorFilterFn(queryOpts.After, queryOpts.Sort),
	)
	filteredIterator := memdb.NewFilterIterator(bestIterator, matchingRelationshipsFilterFunc)

	switch queryOpts.Sort {
	case options.Unsorted:
		fallthrough

	case options.ByResource:
		iter := newMemdbTupleIterator(filteredIterator, queryOpts.Limit, queryOpts.Sort)
		return iter, nil

	case options.BySubject:
		return newSubjectSortedIterator(filteredIterator, queryOpts.Limit)

	default:
		return nil, spiceerrors.MustBugf("unsupported sort order: %v", queryOpts.Sort)
	}
}

func mustHaveBeenClosed(iter *memdbTupleIterator) {
	if !iter.closed {
		panic("Tuple iterator garbage collected before Close() was called")
	}
}

// ReverseQueryRelationships reads relationships starting from the subject.
func (r *memdbReader) ReverseQueryRelationships(
	_ context.Context,
	subjectsFilter datastore.SubjectsFilter,
	opts ...options.ReverseQueryOptionsOption,
) (datastore.RelationshipIterator, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	r.mustLock()
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
		[]datastore.SubjectsSelector{subjectsFilter.AsSelector()},
		"",
		makeCursorFilterFn(queryOpts.AfterForReverse, queryOpts.SortForReverse),
	)
	filteredIterator := memdb.NewFilterIterator(iterator, matchingRelationshipsFilterFunc)

	return newMemdbTupleIterator(filteredIterator, queryOpts.LimitForReverse, queryOpts.SortForReverse), nil
}

// ReadNamespace reads a namespace definition and version and returns it, and the revision at
// which it was created or last written, if found.
func (r *memdbReader) ReadNamespaceByName(_ context.Context, nsName string) (ns *core.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	if r.initErr != nil {
		return nil, datastore.NoRevision, r.initErr
	}

	r.mustLock()
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
func (r *memdbReader) ListAllNamespaces(_ context.Context) ([]datastore.RevisionedNamespace, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	r.mustLock()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	var nsDefs []datastore.RevisionedNamespace

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

		nsDefs = append(nsDefs, datastore.RevisionedNamespace{
			Definition:          loaded,
			LastWrittenRevision: found.updated,
		})
	}

	return nsDefs, nil
}

func (r *memdbReader) LookupNamespacesWithNames(_ context.Context, nsNames []string) ([]datastore.RevisionedNamespace, error) {
	if r.initErr != nil {
		return nil, r.initErr
	}

	if len(nsNames) == 0 {
		return nil, nil
	}

	r.mustLock()
	defer r.Unlock()

	tx, err := r.txSource()
	if err != nil {
		return nil, err
	}

	it, err := tx.LowerBound(tableNamespace, indexID)
	if err != nil {
		return nil, err
	}

	nsNameMap := make(map[string]struct{}, len(nsNames))
	for _, nsName := range nsNames {
		nsNameMap[nsName] = struct{}{}
	}

	nsDefs := make([]datastore.RevisionedNamespace, 0, len(nsNames))

	for foundRaw := it.Next(); foundRaw != nil; foundRaw = it.Next() {
		found := foundRaw.(*namespace)

		loaded := &core.NamespaceDefinition{}
		if err := loaded.UnmarshalVT(found.configBytes); err != nil {
			return nil, err
		}

		if _, ok := nsNameMap[loaded.Name]; ok {
			nsDefs = append(nsDefs, datastore.RevisionedNamespace{
				Definition:          loaded,
				LastWrittenRevision: found.updated,
			})
		}
	}

	return nsDefs, nil
}

func (r *memdbReader) mustLock() {
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
	optionalSubjectsSelectors []datastore.SubjectsSelector,
	optionalCaveatFilter string,
	cursorFilter func(*relationship) bool,
) memdb.FilterFunc {
	return func(tupleRaw interface{}) bool {
		tuple := tupleRaw.(*relationship)

		switch {
		case optionalResourceType != "" && optionalResourceType != tuple.namespace:
			return true
		case len(optionalResourceIds) > 0 && !slices.Contains(optionalResourceIds, tuple.resourceID):
			return true
		case optionalRelation != "" && optionalRelation != tuple.relation:
			return true
		case optionalCaveatFilter != "" && (tuple.caveat == nil || tuple.caveat.caveatName != optionalCaveatFilter):
			return true
		}

		applySubjectSelector := func(selector datastore.SubjectsSelector) bool {
			switch {
			case len(selector.OptionalSubjectType) > 0 && selector.OptionalSubjectType != tuple.subjectNamespace:
				return false
			case len(selector.OptionalSubjectIds) > 0 && !slices.Contains(selector.OptionalSubjectIds, tuple.subjectObjectID):
				return false
			}

			if selector.RelationFilter.OnlyNonEllipsisRelations {
				return tuple.subjectRelation != datastore.Ellipsis
			}

			relations := make([]string, 0, 2)
			if selector.RelationFilter.IncludeEllipsisRelation {
				relations = append(relations, datastore.Ellipsis)
			}

			if selector.RelationFilter.NonEllipsisRelation != "" {
				relations = append(relations, selector.RelationFilter.NonEllipsisRelation)
			}

			return len(relations) == 0 || slices.Contains(relations, tuple.subjectRelation)
		}

		if len(optionalSubjectsSelectors) > 0 {
			hasMatchingSelector := false
			for _, selector := range optionalSubjectsSelectors {
				if applySubjectSelector(selector) {
					hasMatchingSelector = true
					break
				}
			}

			if !hasMatchingSelector {
				return true
			}
		}

		return cursorFilter(tuple)
	}
}

func makeCursorFilterFn(after *core.RelationTuple, order options.SortOrder) func(tpl *relationship) bool {
	if after != nil {
		switch order {
		case options.ByResource:
			return func(tpl *relationship) bool {
				return less(tpl.namespace, tpl.resourceID, tpl.relation, after.ResourceAndRelation) ||
					(eq(tpl.namespace, tpl.resourceID, tpl.relation, after.ResourceAndRelation) &&
						(less(tpl.subjectNamespace, tpl.subjectObjectID, tpl.subjectRelation, after.Subject) ||
							eq(tpl.subjectNamespace, tpl.subjectObjectID, tpl.subjectRelation, after.Subject)))
			}
		case options.BySubject:
			return func(tpl *relationship) bool {
				return less(tpl.subjectNamespace, tpl.subjectObjectID, tpl.subjectRelation, after.Subject) ||
					(eq(tpl.subjectNamespace, tpl.subjectObjectID, tpl.subjectRelation, after.Subject) &&
						(less(tpl.namespace, tpl.resourceID, tpl.relation, after.ResourceAndRelation) ||
							eq(tpl.namespace, tpl.resourceID, tpl.relation, after.ResourceAndRelation)))
			}
		}
	}
	return noopCursorFilter
}

func newSubjectSortedIterator(it memdb.ResultIterator, limit *uint64) (datastore.RelationshipIterator, error) {
	results := make([]*core.RelationTuple, 0)

	// Coalesce all of the results into memory
	for foundRaw := it.Next(); foundRaw != nil; foundRaw = it.Next() {
		rt, err := foundRaw.(*relationship).RelationTuple()
		if err != nil {
			return nil, err
		}

		results = append(results, rt)
	}

	// Sort them by subject
	sort.Slice(results, func(i, j int) bool {
		lhsRes := results[i].ResourceAndRelation
		lhsSub := results[i].Subject
		rhsRes := results[j].ResourceAndRelation
		rhsSub := results[j].Subject
		return less(lhsSub.Namespace, lhsSub.ObjectId, lhsSub.Relation, rhsSub) ||
			(eq(lhsSub.Namespace, lhsSub.ObjectId, lhsSub.Relation, rhsSub) &&
				(less(lhsRes.Namespace, lhsRes.ObjectId, lhsRes.Relation, rhsRes)))
	})

	// Limit them if requested
	if limit != nil && uint64(len(results)) > *limit {
		results = results[0:*limit]
	}

	return common.NewSliceRelationshipIterator(results, options.BySubject), nil
}

func noopCursorFilter(_ *relationship) bool {
	return false
}

func less(lhsNamespace, lhsObjectID, lhsRelation string, rhs *core.ObjectAndRelation) bool {
	return lhsNamespace < rhs.Namespace ||
		(lhsNamespace == rhs.Namespace && lhsObjectID < rhs.ObjectId) ||
		(lhsNamespace == rhs.Namespace && lhsObjectID == rhs.ObjectId && lhsRelation < rhs.Relation)
}

func eq(lhsNamespace, lhsObjectID, lhsRelation string, rhs *core.ObjectAndRelation) bool {
	return lhsNamespace == rhs.Namespace && lhsObjectID == rhs.ObjectId && lhsRelation == rhs.Relation
}

func newMemdbTupleIterator(it memdb.ResultIterator, limit *uint64, order options.SortOrder) *memdbTupleIterator {
	iter := &memdbTupleIterator{it: it, limit: limit, order: order}
	runtime.SetFinalizer(iter, mustHaveBeenClosed)
	return iter
}

type memdbTupleIterator struct {
	closed bool
	it     memdb.ResultIterator
	limit  *uint64
	count  uint64
	err    error
	order  options.SortOrder
	last   *core.RelationTuple
}

func (mti *memdbTupleIterator) Next() *core.RelationTuple {
	if mti.closed {
		return nil
	}

	foundRaw := mti.it.Next()
	if foundRaw == nil {
		return nil
	}

	if mti.limit != nil && mti.count >= *mti.limit {
		return nil
	}
	mti.count++

	rt, err := foundRaw.(*relationship).RelationTuple()
	if err != nil {
		mti.err = err
		return nil
	}

	mti.last = rt
	return rt
}

func (mti *memdbTupleIterator) Cursor() (options.Cursor, error) {
	switch {
	case mti.closed:
		return nil, datastore.ErrClosedIterator
	case mti.order == options.Unsorted:
		return nil, datastore.ErrCursorsWithoutSorting
	case mti.last == nil:
		return nil, datastore.ErrCursorEmpty
	default:
		return mti.last, nil
	}
}

func (mti *memdbTupleIterator) Err() error {
	return mti.err
}

func (mti *memdbTupleIterator) Close() {
	mti.closed = true
	mti.err = datastore.ErrClosedIterator
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
