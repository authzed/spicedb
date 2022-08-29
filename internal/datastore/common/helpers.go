package common

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// WriteTuples is a convenience method to perform the same update operation on a set of tuples
func WriteTuples(ctx context.Context, ds datastore.Datastore, op core.RelationTupleUpdate_Operation, tuples ...*core.RelationTuple) (datastore.Revision, error) {
	updates := make([]*core.RelationTupleUpdate, 0, len(tuples))
	for _, tpl := range tuples {
		rtu := &core.RelationTupleUpdate{
			Operation: op,
			Tuple:     tpl,
		}
		updates = append(updates, rtu)
	}
	return UpdateTuplesInDatastore(ctx, ds, updates...)
}

// UpdateTuplesInDatastore is a convenience method to perform multiple relation update operations on a Datastore
func UpdateTuplesInDatastore(ctx context.Context, ds datastore.Datastore, updates ...*core.RelationTupleUpdate) (datastore.Revision, error) {
	return ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(updates)
	})
}
