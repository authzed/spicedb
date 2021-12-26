package memdb

import (
	"context"
	"errors"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/common"
)

const errWatchError = "watch error: %w"

func (mds *memdbDatastore) Watch(ctx context.Context, afterRevision datastore.Revision) (<-chan *datastore.RevisionChanges, <-chan error) {
	updates := make(chan *datastore.RevisionChanges, mds.watchBufferLength)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		currentTxn := uint64(afterRevision.IntPart())

		for {
			var stagedUpdates []*datastore.RevisionChanges
			var watchChan <-chan struct{}
			var err error
			stagedUpdates, currentTxn, watchChan, err = mds.loadChanges(ctx, currentTxn)
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

func (mds *memdbDatastore) loadChanges(ctx context.Context, currentTxn uint64) ([]*datastore.RevisionChanges, uint64, <-chan struct{}, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return nil, 0, nil, fmt.Errorf("memdb closed")
	}
	loadNewTxn := db.Txn(false)
	defer loadNewTxn.Abort()

	it, err := loadNewTxn.LowerBound(tableTransaction, indexID, currentTxn+1)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	stagedChanges := make(common.Changes)
	for newChangeRaw := it.Next(); newChangeRaw != nil; newChangeRaw = it.Next() {
		currentTxn = newChangeRaw.(*transaction).id
		createdIt, err := loadNewTxn.Get(tableRelationship, indexCreatedTxn, currentTxn)
		if err != nil {
			return nil, 0, nil, fmt.Errorf(errWatchError, err)
		}
		for rawCreated := createdIt.Next(); rawCreated != nil; rawCreated = createdIt.Next() {
			created := rawCreated.(*relationship)
			stagedChanges.AddChange(ctx, currentTxn, created.RelationTuple(), v0.RelationTupleUpdate_TOUCH)
		}

		deletedIt, err := loadNewTxn.Get(tableRelationship, indexDeletedTxn, currentTxn)
		if err != nil {
			return nil, 0, nil, fmt.Errorf(errWatchError, err)
		}
		for rawDeleted := deletedIt.Next(); rawDeleted != nil; rawDeleted = deletedIt.Next() {
			deleted := rawDeleted.(*relationship)
			stagedChanges.AddChange(ctx, currentTxn, deleted.RelationTuple(), v0.RelationTupleUpdate_DELETE)
		}
	}

	watchChan, _, err := loadNewTxn.LastWatch(tableTransaction, indexID)
	if err != nil {
		return nil, 0, nil, fmt.Errorf(errWatchError, err)
	}

	return stagedChanges.AsRevisionChanges(), currentTxn, watchChan, nil
}
