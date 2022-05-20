package common

import (
	"context"
	"fmt"
	"sort"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type revisionKey string

func keyFromRevision(rev datastore.Revision) revisionKey {
	return revisionKey(rev.String())
}

func mustRevisionFromKey(key revisionKey) datastore.Revision {
	rev, err := decimal.NewFromString(string(key))
	if err != nil {
		panic(fmt.Errorf("unparseable revision key(%s): %w", key, err))
	}
	return rev
}

// Changes represents a set of tuple mutations that are kept self-consistent
// across one or more transaction revisions.
type Changes map[revisionKey]*changeRecord

type changeRecord struct {
	tupleTouches map[string]*core.RelationTuple
	tupleDeletes map[string]*core.RelationTuple
}

// NewChanges creates a new Changes object for change tracking and de-duplication.
func NewChanges() Changes {
	return make(Changes)
}

// AddChange adds a specific change to the complete list of tracked changes
func (ch Changes) AddChange(
	ctx context.Context,
	rev decimal.Decimal,
	tpl *core.RelationTuple,
	op core.RelationTupleUpdate_Operation,
) {
	rk := keyFromRevision(rev)
	revisionChanges, ok := ch[rk]
	if !ok {
		revisionChanges = &changeRecord{
			tupleTouches: make(map[string]*core.RelationTuple),
			tupleDeletes: make(map[string]*core.RelationTuple),
		}
		ch[rk] = revisionChanges
	}

	tplKey := tuple.String(tpl)

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
func (ch Changes) AsRevisionChanges() (changes []*datastore.RevisionChanges) {
	type keyAndRevision struct {
		key revisionKey
		rev datastore.Revision
	}

	revisionsWithChanges := make([]keyAndRevision, 0, len(ch))
	for rk := range ch {
		kar := keyAndRevision{rk, mustRevisionFromKey(rk)}
		revisionsWithChanges = append(revisionsWithChanges, kar)
	}
	sort.Slice(revisionsWithChanges, func(i int, j int) bool {
		return revisionsWithChanges[i].rev.LessThan(revisionsWithChanges[j].rev)
	})

	for _, kar := range revisionsWithChanges {
		revisionChange := &datastore.RevisionChanges{
			Revision: kar.rev,
		}

		revisionChangeRecord := ch[kar.key]
		for _, tpl := range revisionChangeRecord.tupleTouches {
			revisionChange.Changes = append(revisionChange.Changes, &core.RelationTupleUpdate{
				Operation: core.RelationTupleUpdate_TOUCH,
				Tuple:     tpl,
			})
		}
		for _, tpl := range revisionChangeRecord.tupleDeletes {
			revisionChange.Changes = append(revisionChange.Changes, &core.RelationTupleUpdate{
				Operation: core.RelationTupleUpdate_DELETE,
				Tuple:     tpl,
			})
		}
		changes = append(changes, revisionChange)
	}

	return
}
