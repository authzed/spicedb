package postgres

import (
	"context"
	"errors"
	"sort"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/rs/zerolog/log"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	watchSleep = 100 * time.Millisecond
)

var (
	queryChanged = psql.Select(
		colNamespace,
		colObjectID,
		colRelation,
		colUsersetNamespace,
		colUsersetObjectID,
		colUsersetRelation,
		colCreatedTxn,
		colDeletedTxn,
	).From(tableTuple)
)

func (pgd *pgDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, pgd.watchBufferLength)
	errors := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errors)

		currentTxn := transactionFromRevision(afterRevision)

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var err error
			stagedUpdates, currentTxn, err = pgd.loadChanges(ctx, currentTxn)
			if err != nil {
				if ctx.Err() == context.Canceled {
					errors <- datastore.NewWatchCanceledErr()
				} else {
					errors <- err
				}
				return
			}

			// Write the staged updates to the channel
			for _, changeToWrite := range stagedUpdates {
				select {
				case updates <- changeToWrite:
				default:
					errors <- datastore.NewWatchDisconnectedErr()
					return
				}
			}

			// If there were no changes, sleep a bit
			if len(stagedUpdates) == 0 {
				sleep := time.NewTimer(watchSleep)

				select {
				case <-sleep.C:
					break
				case <-ctx.Done():
					errors <- datastore.NewWatchCanceledErr()
					return
				}
			}
		}

	}()

	return updates, errors
}

func (pgd *pgDatastore) loadChanges(
	ctx context.Context,
	afterRevision uint64,
) (changes []*datastore.RevisionChanges, newRevision uint64, err error) {

	newRevision, err = pgd.loadRevision(ctx)
	if err != nil {
		return
	}

	if newRevision == afterRevision {
		return
	}

	sql, args, err := queryChanged.Where(sq.Or{
		sq.And{
			sq.Gt{colCreatedTxn: afterRevision},
			sq.LtOrEq{colCreatedTxn: newRevision},
		},
		sq.And{
			sq.Gt{colDeletedTxn: afterRevision},
			sq.LtOrEq{colDeletedTxn: newRevision},
		},
	}).ToSql()
	if err != nil {
		return
	}

	rows, err := pgd.dbpool.Query(ctx, sql, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = datastore.NewWatchCanceledErr()
		}
		return
	}

	stagedChanges := make(map[uint64]*changeRecord)

	for rows.Next() {
		userset := &pb.ObjectAndRelation{}
		tpl := &pb.RelationTuple{
			ObjectAndRelation: &pb.ObjectAndRelation{},
			User: &pb.User{
				UserOneof: &pb.User_Userset{
					Userset: userset,
				},
			},
		}

		var createdTxn uint64
		var deletedTxn uint64
		err = rows.Scan(
			&tpl.ObjectAndRelation.Namespace,
			&tpl.ObjectAndRelation.ObjectId,
			&tpl.ObjectAndRelation.Relation,
			&userset.Namespace,
			&userset.ObjectId,
			&userset.Relation,
			&createdTxn,
			&deletedTxn,
		)
		if err != nil {
			return
		}

		if createdTxn > afterRevision && createdTxn <= newRevision {
			addChange(stagedChanges, createdTxn, tpl, pb.RelationTupleUpdate_TOUCH)
		}

		if deletedTxn > afterRevision && deletedTxn <= newRevision {
			addChange(stagedChanges, deletedTxn, tpl, pb.RelationTupleUpdate_DELETE)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	revisionsWithChanges := make([]uint64, 0, len(stagedChanges))
	for k := range stagedChanges {
		revisionsWithChanges = append(revisionsWithChanges, k)
	}
	sort.Slice(revisionsWithChanges, func(i int, j int) bool {
		return revisionsWithChanges[i] < revisionsWithChanges[j]
	})

	for _, rev := range revisionsWithChanges {
		revisionChange := &datastore.RevisionChanges{
			Revision: revisionFromTransaction(rev),
		}

		revisionChangeRecord := stagedChanges[rev]
		for _, tpl := range revisionChangeRecord.tupleTouches {
			revisionChange.Changes = append(revisionChange.Changes, &pb.RelationTupleUpdate{
				Operation: pb.RelationTupleUpdate_TOUCH,
				Tuple:     tpl,
			})
		}
		for _, tpl := range revisionChangeRecord.tupleDeletes {
			revisionChange.Changes = append(revisionChange.Changes, &pb.RelationTupleUpdate{
				Operation: pb.RelationTupleUpdate_DELETE,
				Tuple:     tpl,
			})
		}
		changes = append(changes, revisionChange)
	}

	return
}

type changeRecord struct {
	tupleTouches map[string]*pb.RelationTuple
	tupleDeletes map[string]*pb.RelationTuple
}

func addChange(
	changes map[uint64]*changeRecord,
	revision uint64,
	tpl *pb.RelationTuple,
	op pb.RelationTupleUpdate_Operation,
) {
	revisionChanges, ok := changes[revision]
	if !ok {
		revisionChanges = &changeRecord{
			tupleTouches: make(map[string]*pb.RelationTuple),
			tupleDeletes: make(map[string]*pb.RelationTuple),
		}
		changes[revision] = revisionChanges
	}

	tplKey := tuple.String(tpl)

	switch op {
	case pb.RelationTupleUpdate_TOUCH:
		// If there was a delete for the same tuple at the same revision, drop it
		delete(revisionChanges.tupleDeletes, tplKey)

		revisionChanges.tupleTouches[tplKey] = tpl

	case pb.RelationTupleUpdate_DELETE:
		_, alreadyTouched := revisionChanges.tupleTouches[tplKey]
		if !alreadyTouched {
			revisionChanges.tupleDeletes[tplKey] = tpl
		}
	default:
		log.Fatal().Stringer("operation", op).Msg("unknown change operation")
	}
}
