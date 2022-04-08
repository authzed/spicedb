package memdb

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-memdb"

	"github.com/authzed/spicedb/internal/datastore"
)

func (mds *memdbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	head, err := mds.HeadRevision(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to compute head revision: %w", err)
	}

	count, err := mds.countRelationships(ctx, head)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to count relationships: %w", err)
	}

	objTypes, err := mds.ListNamespaces(ctx, head)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to list object types: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   mds.uniqueID,
		EstimatedRelationshipCount: count,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(objTypes),
	}, nil
}

func (mds *memdbDatastore) countRelationships(ctx context.Context, revision datastore.Revision) (uint64, error) {
	mds.RLock()
	db := mds.db
	mds.RUnlock()
	if db == nil {
		return 0, fmt.Errorf("memdb closed")
	}

	txn := db.Txn(false)
	defer txn.Abort()

	aliveAndDead, err := txn.LowerBound(tableRelationship, indexID)
	if err != nil {
		return 0, err
	}

	it := memdb.NewFilterIterator(aliveAndDead, filterToLiveObjects(revision))

	var count uint64
	for row := it.Next(); row != nil; row = it.Next() {
		count++
	}

	return count, nil
}
