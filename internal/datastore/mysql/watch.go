package mysql

import (
	"context"
	"errors"
	"iter"
	"time"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	watchSleep = 100 * time.Millisecond
)

func (mds *mysqlDatastore) DefaultsWatchOptions() datastore.WatchOptions {
	return datastore.WatchOptions{
		WatchBufferLength:       defaultWatchBufferLength,
		WatchBufferWriteTimeout: defaultWatchBufferWriteTimeout,
		// MySQL does not use CheckpointInterval or WatchConnectTimeout
		// MySQL does not support EmitImmediatelyStrategy or WatchSchema
	}
}

// Watch notifies the caller about all changes to tuples.
//
// All events following afterRevision will be sent to the caller.
func (mds *mysqlDatastore) Watch(ctx context.Context, afterRevisionRaw datastore.Revision, options datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	updates := make(chan datastore.RevisionChanges, options.WatchBufferLength)
	errs := make(chan error, 1)

	if !mds.watchEnabled {
		close(updates)
		errs <- datastore.NewWatchDisabledErr("watch is disabled")
		return updates, errs
	}

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

	sendChange := func(change datastore.RevisionChanges) bool {
		select {
		case updates <- change:
			return true

		default:
			// If we cannot immediately write, setup the timer and try again.
		}

		timer := time.NewTimer(options.WatchBufferWriteTimeout)
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
			stagedUpdates, ctxn, err := mds.loadChanges(ctx, currentTxn, options)
			if err != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					errs <- datastore.NewWatchCanceledErr()
				} else {
					errs <- err
				}
				return
			}
			previousTxn := currentTxn
			currentTxn = ctxn

			// Write the staged updates to the channel
			changeCount := 0
			for changeToWrite, err := range stagedUpdates {
				if err != nil {
					errs <- err
					return
				}

				if !sendChange(changeToWrite) {
					return
				}
				changeCount++
			}

			// Emit a checkpoint at the latest revision whenever HEAD
			// advances (even even when the transaction produced no observable changes)
			// and the consumer asked for checkpoints.
			if currentTxn > previousTxn &&
				options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints {
				if !sendChange(datastore.RevisionChanges{
					Revision:     revisions.NewForTransactionID(currentTxn),
					IsCheckpoint: true,
				}) {
					return
				}
			}

			// If there were no changes, sleep a bit
			if changeCount == 0 {
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
	options datastore.WatchOptions,
) (iter.Seq2[datastore.RevisionChanges, error], uint64, error) {
	newRevision, err := mds.loadRevision(ctx)
	if err != nil {
		return nil, 0, err
	}

	if newRevision == afterRevision {
		return func(yield func(datastore.RevisionChanges, error) bool) {}, newRevision, nil
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
		return nil, 0, err
	}

	rows, err := mds.db.QueryContext(ctx, sql, args...)
	if err != nil {
		switch {
		case errors.Is(ctx.Err(), context.Canceled):
			err = datastore.NewWatchCanceledErr()
		case common.IsCancellationError(err):
			err = datastore.NewWatchCanceledErr()
		case common.IsResettableError(err):
			err = datastore.NewWatchTemporaryErr(err)
		}
		return nil, 0, err
	}
	defer common.LogOnError(ctx, rows.Close)

	for rows.Next() {
		var txnID uint64
		var metadata common.TransactionMetadata
		err = rows.Scan(
			&txnID,
			&metadata,
		)
		if err != nil {
			return nil, 0, err
		}

		if len(metadata) > 0 {
			if err := stagedChanges.AddRevisionMetadata(ctx, revisions.NewForTransactionID(txnID), metadata); err != nil {
				return nil, 0, err
			}
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return nil, 0, err
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
		return nil, 0, err
	}

	rows, err = mds.db.QueryContext(ctx, sql, args...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			err = datastore.NewWatchCanceledErr()
		}
		return nil, 0, err
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
			return nil, 0, err
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
			return nil, 0, err
		}

		if createdTxn > afterRevision && createdTxn <= newRevision {
			if err = stagedChanges.AddRelationshipChange(ctx, revisions.NewForTransactionID(createdTxn), relationship, tuple.UpdateOperationTouch); err != nil {
				return nil, 0, err
			}
		}

		if deletedTxn > afterRevision && deletedTxn <= newRevision {
			if err = stagedChanges.AddRelationshipChange(ctx, revisions.NewForTransactionID(deletedTxn), relationship, tuple.UpdateOperationDelete); err != nil {
				return nil, 0, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return stagedChanges.AsRevisionChanges(revisions.TransactionIDKeyLessThanFunc), newRevision, nil
}
