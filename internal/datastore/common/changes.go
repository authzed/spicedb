package common

import (
	"context"
	"sort"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Changes represents a set of tuple mutations that are kept self-consistent
// across one or more transaction revisions.
type Changes map[uint64]*changeRecord

type changeRecord struct {
	tupleTouches map[string]*v0.RelationTuple
	tupleDeletes map[string]*v0.RelationTuple
}

// NewChanges creates a new Changes object for change tracking and de-duplication.
func NewChanges() Changes {
	return make(Changes)
}

// AddChange adds a specific change to the complete list of tracked changes
func (ch Changes) AddChange(
	ctx context.Context,
	revTxID uint64,
	tpl *v0.RelationTuple,
	op v0.RelationTupleUpdate_Operation,
) {
	revisionChanges, ok := ch[revTxID]
	if !ok {
		revisionChanges = &changeRecord{
			tupleTouches: make(map[string]*v0.RelationTuple),
			tupleDeletes: make(map[string]*v0.RelationTuple),
		}
		ch[revTxID] = revisionChanges
	}

	tplKey := tuple.String(tpl)

	switch op {
	case v0.RelationTupleUpdate_TOUCH:
		// If there was a delete for the same tuple at the same revision, drop it
		delete(revisionChanges.tupleDeletes, tplKey)

		revisionChanges.tupleTouches[tplKey] = tpl

	case v0.RelationTupleUpdate_DELETE:
		_, alreadyTouched := revisionChanges.tupleTouches[tplKey]
		if !alreadyTouched {
			revisionChanges.tupleDeletes[tplKey] = tpl
		}
	default:
		log.Ctx(ctx).Fatal().Stringer("operation", op).Msg("unknown change operation")
	}
}

// AsRevisionChanges returns the list of changes processes so far as a datastore watch
// compatible, ordered changelist.
func (ch Changes) AsRevisionChanges() (changes []*datastore.RevisionChanges) {
	revisionsWithChanges := make([]uint64, 0, len(ch))
	for revTxID := range ch {
		revisionsWithChanges = append(revisionsWithChanges, revTxID)
	}
	sort.Slice(revisionsWithChanges, func(i int, j int) bool {
		return revisionsWithChanges[i] < revisionsWithChanges[j]
	})

	for _, revTxID := range revisionsWithChanges {
		revisionChange := &datastore.RevisionChanges{
			Revision: revisionFromTransactionID(revTxID),
		}

		revisionChangeRecord := ch[revTxID]
		for _, tpl := range revisionChangeRecord.tupleTouches {
			revisionChange.Changes = append(revisionChange.Changes, &v0.RelationTupleUpdate{
				Operation: v0.RelationTupleUpdate_TOUCH,
				Tuple:     tpl,
			})
		}
		for _, tpl := range revisionChangeRecord.tupleDeletes {
			revisionChange.Changes = append(revisionChange.Changes, &v0.RelationTupleUpdate{
				Operation: v0.RelationTupleUpdate_DELETE,
				Tuple:     tpl,
			})
		}
		changes = append(changes, revisionChange)
	}

	return
}

func revisionFromTransactionID(txID uint64) datastore.Revision {
	return decimal.NewFromInt(int64(txID))
}
