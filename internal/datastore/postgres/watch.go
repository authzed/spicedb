package postgres

import (
	"context"
	"errors"
	"sort"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	errWatchError        = "watch error: %w"
	errWatcherFellBehind = "watcher fell behind, disconnecting"

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

func (pgd *pgDatastore) Watch(ctx context.Context, afterRevision uint64) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, pgd.watchBufferLength)
	errors := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errors)

		currentTxn := afterRevision

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var err error
			stagedUpdates, currentTxn, err = pgd.loadChanges(ctx, currentTxn)
			if err != nil {
				if ctx.Err() == context.Canceled {
					errors <- datastore.ErrWatchCanceled
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
					errors <- datastore.ErrWatchDisconnected
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
					errors <- datastore.ErrWatchCanceled
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

	rows, err := pgd.db.QueryxContext(ctx, sql, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = datastore.ErrWatchCanceled
		}
		return
	}

	stagedChanges := make(map[uint64]*datastore.RevisionChanges, newRevision-afterRevision)

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
		rows.Scan(
			&tpl.ObjectAndRelation.Namespace,
			&tpl.ObjectAndRelation.ObjectId,
			&tpl.ObjectAndRelation.Relation,
			&userset.Namespace,
			&userset.ObjectId,
			&userset.Relation,
			&createdTxn,
			&deletedTxn,
		)

		if createdTxn > afterRevision {
			addChange(stagedChanges, createdTxn, &pb.RelationTupleUpdate{
				Operation: pb.RelationTupleUpdate_CREATE,
				Tuple:     tpl,
			})
		}

		if deletedTxn > afterRevision && deletedTxn < liveDeletedTxnID {
			addChange(stagedChanges, createdTxn, &pb.RelationTupleUpdate{
				Operation: pb.RelationTupleUpdate_DELETE,
				Tuple:     tpl,
			})
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
		changes = append(changes, stagedChanges[rev])
	}

	return
}

func addChange(changes map[uint64]*datastore.RevisionChanges, revision uint64, change *pb.RelationTupleUpdate) {
	revisionChanges, ok := changes[revision]
	if !ok {
		revisionChanges = &datastore.RevisionChanges{
			Revision: revision,
		}
		changes[revision] = revisionChanges
	}

	revisionChanges.Changes = append(revisionChanges.Changes, change)
}
