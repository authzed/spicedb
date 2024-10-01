package common

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// WriteRelationships is a convenience method to perform the same update operation on a set of relationships
func WriteRelationships(ctx context.Context, ds datastore.Datastore, op tuple.UpdateOperation, rels ...tuple.Relationship) (datastore.Revision, error) {
	updates := make([]tuple.RelationshipUpdate, 0, len(rels))
	for _, rel := range rels {
		ru := tuple.RelationshipUpdate{
			Operation:    op,
			Relationship: rel,
		}
		updates = append(updates, ru)
	}
	return UpdateRelationshipsInDatastore(ctx, ds, updates...)
}

// UpdateRelationshipsInDatastore is a convenience method to perform multiple relation update operations on a Datastore
func UpdateRelationshipsInDatastore(ctx context.Context, ds datastore.Datastore, updates ...tuple.RelationshipUpdate) (datastore.Revision, error) {
	return ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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
