package shared

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/relationships"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schema/v2"
	schematesting "github.com/authzed/spicedb/pkg/schema/v2/testing"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// applySchemaForTesting writes the schema, by way of its schema text, returning the error
// from applying it, if any.
func applySchemaForTesting(t *rapid.T, dl datalayer.DataLayer, s *schema.Schema) error {
	objectDefs, _, err := s.ToDefinitions()
	require.NoError(t, err)

	definitions := make([]compiler.SchemaDefinition, 0, len(objectDefs))
	for _, objectDef := range objectDefs {
		definitions = append(definitions, objectDef)
	}

	schemaText, _, err := generator.GenerateSchema(t.Context(), definitions)
	require.NoError(t, err)

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	validated, err := ValidateSchemaChanges(t.Context(), compiled, caveattypes.Default.TypeSet, false, schemaText)
	require.NoError(t, err)

	_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		_, err := ApplySchemaChanges(ctx, rwt, caveattypes.Default.TypeSet, validated)
		return err
	})
	return err
}

// validateRelationshipsForTesting returns whether the relationships are valid to be written
// under the current schema.
func validateRelationshipsForTesting(t *rapid.T, dl datalayer.DataLayer, rels ...tuple.Relationship) error {
	var validationErr error
	_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		sr, err := rwt.ReadSchema(ctx)
		if err != nil {
			return err
		}

		validationErr = relationships.ValidateRelationshipsForCreateOrTouch(ctx, sr, caveattypes.Default.TypeSet, rels...)
		return nil
	})
	require.NoError(t, err)
	return validationErr
}

func updateRelationshipsForTesting(t *rapid.T, dl datalayer.DataLayer, operation tuple.UpdateOperation, rels []tuple.Relationship) {
	if len(rels) == 0 {
		return
	}

	updates := make([]tuple.RelationshipUpdate, 0, len(rels))
	for _, rel := range rels {
		updates = append(updates, tuple.RelationshipUpdate{Operation: operation, Relationship: rel})
	}

	_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, updates)
	})
	require.NoError(t, err)
}

func readRelationshipsForTesting(t *rapid.T, dl datalayer.DataLayer, s *schema.Schema) []string {
	revision, schemaHash, err := dl.HeadRevision(t.Context())
	require.NoError(t, err)

	var found []string
	reader := dl.SnapshotReader(revision, schemaHash)
	for definitionName := range s.Definitions() {
		iter, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: definitionName})
		require.NoError(t, err)

		rels, err := datastore.IteratorToSlice(iter)
		require.NoError(t, err)

		for _, rel := range rels {
			found = append(found, rel.String())
		}
	}
	return found
}

// TestApplySchemaChangesMatchesVariant ensures that applying a change to a schema is rejected
// if, and only if, there are relationships that the change would leave without a place in the
// schema, and that the change is accepted once those relationships have been removed.
func TestApplySchemaChangesMatchesVariant(t *testing.T) {
	objectIDs := []string{"o_a", "o_b", "o_c"}

	schematesting.CheckWithSchemaVariant(t, func(rt *rapid.T, v schematesting.Variant, baseRelationships schematesting.RelationshipGenerator) {
		dl, err := dsfortesting.DataLayerForTesting(t, 0, 0, memdb.DisableGC)
		require.NoError(rt, err)

		require.NoError(rt, applySchemaForTesting(rt, dl, v.Base))

		// Generate relationships for the base schema, including some that the variant is
		// known to leave without a place in its schema.
		var rels []tuple.Relationship
		seen := map[string]struct{}{}
		addRelationships := func(generated func(yield func(tuple.Relationship) bool), count int) {
			if count == 0 {
				return
			}

			added := 0
			for rel := range generated {
				if _, ok := seen[rel.String()]; !ok {
					seen[rel.String()] = struct{}{}
					rels = append(rels, rel)
				}

				added++
				if added >= count {
					break
				}
			}
		}

		// NOTE: a small set of object IDs is used so that relationships share objects.
		v = v.WithObjectIDs(objectIDs...)
		baseRelationships = baseRelationships.WithObjectIDs(objectIDs...)

		addRelationships(baseRelationships.GenerateRelationships(rt), rapid.IntRange(0, 10).Draw(rt, "relationshipCount"))
		addRelationships(v.GenerateRemovedRelationships(rt), rapid.IntRange(0, 3).Draw(rt, "removedRelationshipCount"))

		require.NoError(rt, validateRelationshipsForTesting(rt, dl, rels...))
		updateRelationshipsForTesting(rt, dl, tuple.UpdateOperationTouch, rels)

		// Apply the variant with all the relationships in place.
		kept, removed := v.Partition(rels)
		err = applySchemaForTesting(rt, dl, v.Schema)
		if len(removed) == 0 {
			require.NoError(rt, err)
		} else {
			var dataValidationErr SchemaWriteDataValidationError
			require.Error(rt, err, "expected schema to be rejected due to %v", removed)
			require.ErrorAs(rt, err, &dataValidationErr)

			// Apply the variant with the relationships removed.
			updateRelationshipsForTesting(rt, dl, tuple.UpdateOperationDelete, removed)
			require.NoError(rt, applySchemaForTesting(rt, dl, v.Schema))
		}

		// The relationships kept are valid under the variant, and those removed are not.
		for _, rel := range kept {
			require.NoError(rt, validateRelationshipsForTesting(rt, dl, rel))
		}
		for _, rel := range removed {
			require.Error(rt, validateRelationshipsForTesting(rt, dl, rel), "expected %s to be invalid", rel)
		}

		// Relationships that the variant newly allows can be written.
		var added []tuple.Relationship
		for rel := range v.GenerateAddedRelationships(rt) {
			added = append(added, rel)
			if len(added) >= 3 {
				break
			}
		}

		require.NoError(rt, validateRelationshipsForTesting(rt, dl, added...))
		updateRelationshipsForTesting(rt, dl, tuple.UpdateOperationTouch, added)

		found := readRelationshipsForTesting(rt, dl, v.Schema)
		for _, rel := range kept {
			require.Contains(rt, found, rel.String())
		}
		for _, rel := range added {
			require.Contains(rt, found, rel.String())
		}

		// Rolling the change back is rejected for as long as the relationships that only
		// the variant allows are in place.
		inverse := v.Inverse()
		_, removedOnRollback := inverse.Partition(append(slices.Clone(kept), added...))
		err = applySchemaForTesting(rt, dl, inverse.Schema)
		if len(removedOnRollback) == 0 {
			require.NoError(rt, err)
			return
		}

		var dataValidationErr SchemaWriteDataValidationError
		require.Error(rt, err, "expected rollback to be rejected due to %v", removedOnRollback)
		require.ErrorAs(rt, err, &dataValidationErr)

		updateRelationshipsForTesting(rt, dl, tuple.UpdateOperationDelete, removedOnRollback)
		require.NoError(rt, applySchemaForTesting(rt, dl, inverse.Schema))
	})
}
