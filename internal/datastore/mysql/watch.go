package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
)

const (
	watchSleep = 100 * time.Millisecond
)

// Watch notifies the caller about all changes to tuples.
//
// All events following afterRevision will be sent to the caller.
//
// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) Watch(ctx context.Context, afterRevisionRaw datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	afterRevision := afterRevisionRaw.(revision.Decimal)

	updates := make(chan *datastore.RevisionChanges, mds.watchBufferLength)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := transactionFromRevision(afterRevision)

		for {
			var stagedUpdates []datastore.RevisionChanges
			var err error
			stagedUpdates, currentTxn, err = mds.loadChanges(ctx, currentTxn)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}

			// Write the staged updates to the channel
			for _, changeToWrite := range stagedUpdates {
				changeToWrite := changeToWrite

				select {
				case updates <- &changeToWrite:
				default:
					errs <- datastore.NewWatchDisconnectedErr()
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
					errs <- datastore.NewWatchCanceledErr()
					return
				}
			}
		}
	}()

	return updates, errs
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func (mds *Datastore) loadChanges(
	ctx context.Context,
	afterRevision uint64,
) (changes []datastore.RevisionChanges, newRevision uint64, err error) {
	newRevision, err = mds.loadRevision(ctx)
	if err != nil {
		return
	}

	if newRevision == afterRevision {
		return
	}

	sql, args, err := mds.QueryChangedQuery.Where(sq.Or{
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

	rows, err := mds.db.QueryContext(ctx, sql, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = datastore.NewWatchCanceledErr()
		}
		return
	}
	defer common.LogOnError(ctx, rows.Close)

	stagedChanges := common.NewChanges(revision.DecimalKeyFunc)

	for rows.Next() {
		nextTuple := &core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}

		var createdTxn uint64
		var deletedTxn uint64
		var caveatName string
		var caveatContext caveatContextWrapper
		err = rows.Scan(
			&nextTuple.ResourceAndRelation.Namespace,
			&nextTuple.ResourceAndRelation.ObjectId,
			&nextTuple.ResourceAndRelation.Relation,
			&nextTuple.Subject.Namespace,
			&nextTuple.Subject.ObjectId,
			&nextTuple.Subject.Relation,
			&caveatName,
			&caveatContext,
			&createdTxn,
			&deletedTxn,
		)
		if err != nil {
			return
		}
		nextTuple.Caveat, err = common.ContextualizedCaveatFrom(caveatName, caveatContext)
		if err != nil {
			return
		}

		if createdTxn > afterRevision && createdTxn <= newRevision {
			stagedChanges.AddChange(ctx, revisionFromTransaction(createdTxn), nextTuple, core.RelationTupleUpdate_TOUCH)
		}

		if deletedTxn > afterRevision && deletedTxn <= newRevision {
			stagedChanges.AddChange(ctx, revisionFromTransaction(deletedTxn), nextTuple, core.RelationTupleUpdate_DELETE)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	changes = stagedChanges.AsRevisionChanges(revision.DecimalKeyLessThanFunc)

	return
}
