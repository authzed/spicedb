package mysql

import (
	"context"
	"errors"
	"time"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"

	sq "github.com/Masterminds/squirrel"
)

const (
	watchSleep = 100 * time.Millisecond
)

// Watch notifies the caller about all changes to tuples.
//
// All events following afterRevision will be sent to the caller.
func (mds *Datastore) Watch(ctx context.Context, afterRevisionRaw datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	watchBufferLength := options.WatchBufferLength
	if watchBufferLength <= 0 {
		watchBufferLength = mds.watchBufferLength
	}

	updates := make(chan datastore.RevisionChanges, watchBufferLength)
	errs := make(chan error, 1)

	if options.Content&datastore.WatchSchema == datastore.WatchSchema {
		close(updates)
		errs <- errors.New("schema watch unsupported in MySQL")
		return updates, errs
	}

	if options.EmissionStrategy == datastore.EmitImmediatelyStrategy {
		close(updates)
		errs <- errors.New("emit immediately strategy is unsupported in MySQL")
		return updates, errs
	}

	afterRevision, ok := afterRevisionRaw.(revisions.TransactionIDRevision)
	if !ok {
		errs <- datastore.NewInvalidRevisionErr(afterRevisionRaw, datastore.CouldNotDetermineRevision)
		return updates, errs
	}

	watchBufferWriteTimeout := options.WatchBufferWriteTimeout
	if watchBufferWriteTimeout <= 0 {
		watchBufferWriteTimeout = mds.watchBufferWriteTimeout
	}

	sendChange := func(change datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true

		default:
			// If we cannot immediately write, setup the timer and try again.
		}

		timer := time.NewTimer(watchBufferWriteTimeout)
		defer timer.Stop()

		select {
		case updates <- change:
			return true

		case <-timer.C:
			errs <- datastore.NewWatchDisconnectedErr()
			return false
		}
	}

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := afterRevision.TransactionID()
		for {
			var stagedUpdates []datastore.RevisionChanges
			var err error
			stagedUpdates, currentTxn, err = mds.loadChanges(ctx, currentTxn, options)
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
				if !sendChange(changeToWrite) {
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

func (mds *Datastore) loadChanges(
	ctx context.Context,
	afterRevision uint64,
	options datastore.WatchOptions,
) (changes []datastore.RevisionChanges, newRevision uint64, err error) {
	newRevision, err = mds.loadRevision(ctx)
	if err != nil {
		return
	}

	if newRevision == afterRevision {
		return
	}

	stagedChanges := common.NewChanges(revisions.TransactionIDKeyFunc, options.Content, options.MaximumBufferedChangesByteSize)

	// Load any metadata for the revision range.
	sql, args, err := mds.LoadRevisionRange.Where(sq.Or{
		sq.And{
			sq.Gt{colID: afterRevision},
			sq.LtOrEq{colID: newRevision},
		},
	}).ToSql()
	if err != nil {
		return
	}

	rows, err := mds.db.QueryContext(ctx, sql, args...)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			err = datastore.NewWatchCanceledErr()
		} else if common.IsCancellationError(err) {
			err = datastore.NewWatchCanceledErr()
		} else if common.IsResettableError(err) {
			err = datastore.NewWatchTemporaryErr(err)
		}
		return
	}
	defer common.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var txnID uint64
		var metadata structpbWrapper
		err = rows.Scan(
			&txnID,
			&metadata,
		)
		if err != nil {
			return nil, 0, err
		}

		if len(metadata) > 0 {
			if err := stagedChanges.SetRevisionMetadata(ctx, revisions.NewForTransactionID(txnID), metadata); err != nil {
				return nil, 0, err
			}
		}
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return
	}

	// Load the changes relationships for the revision range.
	sql, args, err = mds.QueryChangedQuery.Where(sq.Or{
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

	rows, err = mds.db.QueryContext(ctx, sql, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = datastore.NewWatchCanceledErr()
		}
		return
	}
	defer common.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var resourceObjectType string
		var resourceObjectID string
		var relation string
		var subjectObjectType string
		var subjectObjectID string
		var subjectRelation string
		var createdTxn uint64
		var deletedTxn uint64
		var caveatName string
		var caveatContext structpbWrapper
		var expiration *time.Time
		err = rows.Scan(
			&resourceObjectType,
			&resourceObjectID,
			&relation,
			&subjectObjectType,
			&subjectObjectID,
			&subjectRelation,
			&caveatName,
			&caveatContext,
			&expiration,
			&createdTxn,
			&deletedTxn,
		)
		if err != nil {
			return
		}

		relationship := tuple.Relationship{
			RelationshipReference: tuple.RelationshipReference{
				Resource: tuple.ObjectAndRelation{
					ObjectType: resourceObjectType,
					ObjectID:   resourceObjectID,
					Relation:   relation,
				},
				Subject: tuple.ObjectAndRelation{
					ObjectType: subjectObjectType,
					ObjectID:   subjectObjectID,
					Relation:   subjectRelation,
				},
			},
			OptionalExpiration: expiration,
		}

		relationship.OptionalCaveat, err = common.ContextualizedCaveatFrom(caveatName, caveatContext)
		if err != nil {
			return
		}

		if createdTxn > afterRevision && createdTxn <= newRevision {
			if err = stagedChanges.AddRelationshipChange(ctx, revisions.NewForTransactionID(createdTxn), relationship, tuple.UpdateOperationTouch); err != nil {
				return
			}
		}

		if deletedTxn > afterRevision && deletedTxn <= newRevision {
			if err = stagedChanges.AddRelationshipChange(ctx, revisions.NewForTransactionID(deletedTxn), relationship, tuple.UpdateOperationDelete); err != nil {
				return
			}
		}
	}
	if err = rows.Err(); err != nil {
		return
	}

	changes, err = stagedChanges.AsRevisionChanges(revisions.TransactionIDKeyLessThanFunc)
	return
}
