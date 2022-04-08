package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/mysql/migrations"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"

	sq "github.com/Masterminds/squirrel"
)

const (
	watchSleep = 100 * time.Millisecond
)

// Watch notifies the caller about all changes to tuples.
//
// All events following afterRevision will be sent to the caller.
func (mds *mysqlDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, mds.watchBufferLength)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := transactionFromRevision(afterRevision)

		for {
			var stagedUpdates []*datastore.RevisionChanges
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
				select {
				case updates <- changeToWrite:
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

func (mds *mysqlDatastore) loadChanges(
	ctx context.Context,
	afterRevision uint64,
) (changes []*datastore.RevisionChanges, newRevision uint64, err error) {
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
	defer migrations.LogOnError(ctx, rows.Close)

	stagedChanges := common.NewChanges()

	for rows.Next() {
		userset := &corev1.ObjectAndRelation{}
		tpl := &corev1.RelationTuple{
			ObjectAndRelation: &corev1.ObjectAndRelation{},
			User: &corev1.User{
				UserOneof: &corev1.User_Userset{
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
			stagedChanges.AddChange(ctx, revisionFromTransaction(createdTxn), tpl, corev1.RelationTupleUpdate_TOUCH)
		}

		if deletedTxn > afterRevision && deletedTxn <= newRevision {
			stagedChanges.AddChange(ctx, revisionFromTransaction(deletedTxn), tpl, corev1.RelationTupleUpdate_DELETE)
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	changes = stagedChanges.AsRevisionChanges()

	return
}
