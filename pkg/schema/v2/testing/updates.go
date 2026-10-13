package testing

import (
	"slices"

	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/tuple"
)

func drawUpdateBatches(t *rapid.T, initial, candidates []tuple.Relationship) [][]tuple.RelationshipUpdate {
	if len(candidates) == 0 {
		return nil
	}
	state := make(map[tuple.RelationshipReference]bool, len(initial))
	for _, rel := range initial {
		state[rel.RelationshipReference] = true
	}
	var batches [][]tuple.RelationshipUpdate
	if len(initial) > 0 {
		indices := rapid.SliceOfNDistinct(rapid.IntRange(0, len(initial)-1), 1, min(3, len(initial)), rapid.ID[int]).Draw(t, "transitionRelationships")
		// The first four transactions intentionally restore exactly the initial
		// graph. The second deletion and the initial touch are idempotent.
		for _, operation := range []tuple.UpdateOperation{
			tuple.UpdateOperationTouch, tuple.UpdateOperationDelete,
			tuple.UpdateOperationDelete, tuple.UpdateOperationCreate,
		} {
			batch := make([]tuple.RelationshipUpdate, 0, len(indices))
			for _, index := range indices {
				batch = append(batch, tuple.RelationshipUpdate{Operation: operation, Relationship: initial[index]})
			}
			batches = append(batches, batch)
		}
	}
	for range rapid.IntRange(0, 5).Draw(t, "randomBatches") {
		indices := rapid.SliceOfNDistinct(rapid.IntRange(0, len(candidates)-1), 1, min(4, len(candidates)), rapid.ID[int]).Draw(t, "batchRelationships")
		// Stable ordering gives smaller, easier-to-read reproducers without
		// relying on Go map iteration order for any random choice.
		slices.Sort(indices)
		batch := make([]tuple.RelationshipUpdate, 0, len(indices))
		for _, index := range indices {
			rel := candidates[index]
			operations := []tuple.UpdateOperation{tuple.UpdateOperationTouch, tuple.UpdateOperationDelete}
			if !state[rel.RelationshipReference] {
				operations = append(operations, tuple.UpdateOperationCreate)
			}
			operation := rapid.SampledFrom(operations).Draw(t, "updateOperation")
			batch = append(batch, tuple.RelationshipUpdate{Operation: operation, Relationship: rel})
			if operation == tuple.UpdateOperationDelete {
				delete(state, rel.RelationshipReference)
			} else {
				state[rel.RelationshipReference] = true
			}
		}
		batches = append(batches, batch)
	}
	return batches
}
