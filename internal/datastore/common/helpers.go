package common

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

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
	return ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, updates)
	})
}

// ContextualizedCaveatFrom convenience method that handles creation of a contextualized caveat
// given the possibility of arguments with zero-values.
func ContextualizedCaveatFrom(name string, context map[string]any) (*core.ContextualizedCaveat, error) {
	var caveat *core.ContextualizedCaveat
	if name != "" {
		strct, err := structpb.NewStruct(context)
		if err != nil {
			return nil, fmt.Errorf("malformed caveat context: %w", err)
		}
		caveat = &core.ContextualizedCaveat{
			CaveatName: name,
			Context:    strct,
		}
	}
	return caveat, nil
}
