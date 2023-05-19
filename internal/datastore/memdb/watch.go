package memdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
)

const errWatchError = "watch error: %w"

func (mdb *memdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	ar := afterRevision.(revision.Decimal)

	updates := make(chan *datastore.RevisionChanges, mdb.watchBufferLength)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := ar.IntPart()

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var watchChan <-chan struct{}
			var err error
			stagedUpdates, currentTxn, watchChan, err = mdb.loadChanges(ctx, currentTxn)
			if err != nil {
				errs <- err
				return
			}

			// Write the staged updates to the channel
			for _, changeToWrite := range stagedUpdates {
				if len(changeToWrite.Changes) == 0 {
					continue
				}

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

func (mdb *memdbDatastore) loadChanges(_ context.Context, currentTxn int64) ([]*datastore.RevisionChanges, int64, <-chan struct{}, error) {
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
		changes = append(changes, &change.changes)
		lastRevision = change.revisionNanos
	}

	watchChan, _, err := loadNewTxn.LastWatch(tableChangelog, indexRevision)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	return changes, lastRevision, watchChan, nil
}
