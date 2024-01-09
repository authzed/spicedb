package memdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

const errWatchError = "watch error: %w"

func (mdb *memdbDatastore) Watch(ctx context.Context, ar datastore.Revision, options datastore.WatchOptions) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, mdb.watchBufferLength)
	errs := make(chan error, 1)

	if options.Content&datastore.WatchSchema == datastore.WatchSchema {
		errs <- errors.New("schema watch unsupported in MemDB")
		return updates, errs
	}

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := ar.(revisions.TimestampRevision).TimestampNanoSec()

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var watchChan <-chan struct{}
			var err error
			stagedUpdates, currentTxn, watchChan, err = mdb.loadChanges(ctx, currentTxn, options)
			if err != nil {
				errs <- err
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

			// Wait for new changes
			ws := memdb.NewWatchSet()
			ws.Add(watchChan)

			err = ws.WatchCtx(ctx)
			if err != nil {
				switch {
				case errors.Is(err, context.Canceled):
					errs <- datastore.NewWatchCanceledErr()
				default:
					errs <- fmt.Errorf(errWatchError, err)
				}
				return
			}
		}
	}()

	return updates, errs
}

func (mdb *memdbDatastore) loadChanges(_ context.Context, currentTxn int64, options datastore.WatchOptions) ([]*datastore.RevisionChanges, int64, <-chan struct{}, error) {
	mdb.RLock()
	defer mdb.RUnlock()

	loadNewTxn := mdb.db.Txn(false)
	defer loadNewTxn.Abort()

	it, err := loadNewTxn.LowerBound(tableChangelog, indexRevision, currentTxn+1)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	var changes []*datastore.RevisionChanges
	lastRevision := currentTxn
	for changeRaw := it.Next(); changeRaw != nil; changeRaw = it.Next() {
		change := changeRaw.(*changelog)

		if options.Content&datastore.WatchRelationships == datastore.WatchRelationships && len(change.changes.RelationshipChanges) > 0 {
			changes = append(changes, &change.changes)
		}

		if options.Content&datastore.WatchCheckpoints == datastore.WatchCheckpoints && change.revisionNanos > lastRevision {
			changes = append(changes, &datastore.RevisionChanges{
				Revision:     revisions.NewForTimestamp(change.revisionNanos),
				IsCheckpoint: true,
			})
		}

		if options.Content&datastore.WatchSchema == datastore.WatchSchema &&
			len(change.changes.ChangedDefinitions) > 0 || len(change.changes.DeletedCaveats) > 0 || len(change.changes.DeletedNamespaces) > 0 {
			changes = append(changes, &change.changes)
		}

		lastRevision = change.revisionNanos
	}

	watchChan, _, err := loadNewTxn.LastWatch(tableChangelog, indexRevision)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	return changes, lastRevision, watchChan, nil
}
