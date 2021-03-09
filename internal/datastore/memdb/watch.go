package memdb

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
)

func (mds *memdbDatastore) Watch(ctx context.Context, afterRevision uint64) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, mds.watchBufferLength)
	errors := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errors)

		currentTxn := afterRevision

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var watchChan <-chan struct{}
			var err error
			stagedUpdates, currentTxn, watchChan, err = mds.loadChanges(currentTxn)
			if err != nil {
				errors <- err
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

			// Wait for new changes
			ws := memdb.NewWatchSet()
			ws.Add(watchChan)

			err = ws.WatchCtx(ctx)
			if err != nil {
				switch err {
				case context.Canceled:
					errors <- datastore.ErrWatchCanceled
				default:
					errors <- fmt.Errorf(errWatchError, err)
				}
				return
			}
		}
	}()

	return updates, errors
}

func (mds *memdbDatastore) loadChanges(currentTxn uint64) ([]*datastore.RevisionChanges, uint64, <-chan struct{}, error) {
	loadNewTxn := mds.db.Txn(false)
	defer loadNewTxn.Abort()

	it, err := loadNewTxn.LowerBound(tableChangelog, indexID, currentTxn+1)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	var stagedUpdates []*datastore.RevisionChanges
	for newChangeRaw := it.Next(); newChangeRaw != nil; newChangeRaw = it.Next() {
		newChange := newChangeRaw.(*tupleChangelog)
		stagedUpdates = append(stagedUpdates, &datastore.RevisionChanges{
			Revision: newChange.id,
			Changes:  newChange.changes,
		})
		currentTxn = newChange.id
	}

	watchChan, _, err := loadNewTxn.LastWatch(tableChangelog, indexID)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	return stagedUpdates, currentTxn, watchChan, nil
}
