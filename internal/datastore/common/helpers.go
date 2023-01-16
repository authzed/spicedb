package common

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
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

// CreateRelationshipExistsError is an error returned when attempting to CREATE an already-existing
// relationship.
type CreateRelationshipExistsError struct {
	error

	// Relationship is the relationship that caused the error. May be nil, depending on the datastore.
	Relationship *core.RelationTuple
}

// NewCreateRelationshipExistsError creates a new CreateRelationshipExistsError.
func NewCreateRelationshipExistsError(relationship *core.RelationTuple) error {
	msg := "could not CREATE one or more relationships, as they already existed. If this is persistent, please switch to TOUCH operations or specify a precondition"
	if relationship != nil {
		msg = fmt.Sprintf("could not CREATE relationship `%s`, as it already existed. If this is persistent, please switch to TOUCH operations or specify a precondition", tuple.StringWithoutCaveat(relationship))
	}

	return CreateRelationshipExistsError{
		fmt.Errorf(msg),
		relationship,
	}
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
