package common

import (
	"context"
	"sort"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Changes represents a set of tuple mutations that are kept self-consistent
// across one or more transaction revisions.
type Changes[R datastore.Revision, K comparable] struct {
	records map[K]changeRecord[R]
	keyFunc func(R) K
}

type changeRecord[R datastore.Revision] struct {
	rev          R
	tupleTouches map[string]*core.RelationTuple
	tupleDeletes map[string]*core.RelationTuple
}

// NewChanges creates a new Changes object for change tracking and de-duplication.
func NewChanges[R datastore.Revision, K comparable](keyFunc func(R) K) Changes[R, K] {
	return Changes[R, K]{
		make(map[K]changeRecord[R], 0),
		keyFunc,
	}
}

// AddChange adds a specific change to the complete list of tracked changes
func (ch Changes[R, K]) AddChange(
	ctx context.Context,
	rev R,
	tpl *core.RelationTuple,
	op core.RelationTupleUpdate_Operation,
) {
	k := ch.keyFunc(rev)
	revisionChanges, ok := ch.records[k]
	if !ok {
		revisionChanges = changeRecord[R]{
			rev,
			make(map[string]*core.RelationTuple),
			make(map[string]*core.RelationTuple),
		}
		ch.records[k] = revisionChanges
	}

	tplKey := tuple.StringWithoutCaveat(tpl)

	switch op {
	case core.RelationTupleUpdate_TOUCH:
		// If there was a delete for the same tuple at the same revision, drop it
		delete(revisionChanges.tupleDeletes, tplKey)

		revisionChanges.tupleTouches[tplKey] = tpl

	case core.RelationTupleUpdate_DELETE:
		_, alreadyTouched := revisionChanges.tupleTouches[tplKey]
		if !alreadyTouched {
			revisionChanges.tupleDeletes[tplKey] = tpl
		}
	default:
		log.Ctx(ctx).Fatal().Stringer("operation", op).Msg("unknown change operation")
	}
}

// AsRevisionChanges returns the list of changes processed so far as a datastore watch
// compatible, ordered, changelist.
func (ch Changes[R, K]) AsRevisionChanges(lessThanFunc func(lhs, rhs K) bool) []datastore.RevisionChanges {
	revisionsWithChanges := make([]K, 0, len(ch.records))
	for rk := range ch.records {
		revisionsWithChanges = append(revisionsWithChanges, rk)
	}
	sort.Slice(revisionsWithChanges, func(i int, j int) bool {
		return lessThanFunc(revisionsWithChanges[i], revisionsWithChanges[j])
	})

	changes := make([]datastore.RevisionChanges, len(revisionsWithChanges))

	for i, k := range revisionsWithChanges {
		revisionChangeRecord := ch.records[k]
		changes[i].Revision = revisionChangeRecord.rev
		for _, tpl := range revisionChangeRecord.tupleTouches {
			changes[i].Changes = append(changes[i].Changes, &core.RelationTupleUpdate{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tpl,
			})
		}
		for _, tpl := range revisionChangeRecord.tupleDeletes {
			changes[i].Changes = append(changes[i].Changes, &core.RelationTupleUpdate{
				Operation: core.RelationTupleUpdate_DELETE,
				Tuple:     tpl,
			})
		}
	}

	return changes
}
