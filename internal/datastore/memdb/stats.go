package memdb

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
)

func (mdb *memdbDatastore) Statistics(ctx context.Context) (datastore.Stats, error) {
	head, err := mdb.HeadRevision(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to compute head revision: %w", err)
	}

	count, err := mdb.countRelationships(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to count relationships: %w", err)
	}

	objTypes, err := mdb.SnapshotReader(head).ListAllNamespaces(ctx)
	if err != nil {
		return datastore.Stats{}, fmt.Errorf("unable to list object types: %w", err)
	}

	return datastore.Stats{
		UniqueID:                   mdb.uniqueID,
		EstimatedRelationshipCount: count,
		ObjectTypeStatistics:       datastore.ComputeObjectTypeStats(objTypes),
	}, nil
}

func (mdb *memdbDatastore) countRelationships(_ context.Context) (uint64, error) {
	mdb.RLock()
	defer mdb.RUnlock()

	txn := mdb.db.Txn(false)
	defer txn.Abort()

	it, err := txn.LowerBound(tableRelationship, indexID)
	if err != nil {
		return 0, err
	}

	var count uint64
	for row := it.Next(); row != nil; row = it.Next() {
		count++
	}

	return count, nil
}
