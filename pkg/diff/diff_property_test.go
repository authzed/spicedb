package diff

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/diff/namespace"
	"github.com/authzed/spicedb/pkg/schema/v2"
	schematesting "github.com/authzed/spicedb/pkg/schema/v2/testing"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// expectedChanges are the changes that a diff is expected to report.
type expectedChanges struct {
	addedNamespaces   []string
	removedNamespaces []string

	// changedNamespaces holds the sorted deltas expected for each changed namespace, with
	// each in the form `delta-type:relation-name[:subject-type#subject-relation]`.
	changedNamespaces map[string][]string
}

func diffableFromSchema(t *rapid.T, s *schema.Schema) DiffableSchema {
	objectDefs, caveatDefs, err := s.ToDefinitions()
	require.NoError(t, err)
	return DiffableSchema{ObjectDefinitions: objectDefs, CaveatDefinitions: caveatDefs}
}

// diffableFromSchemaText returns the schema as it is once written as, and compiled from,
// schema text.
func diffableFromSchemaText(t *rapid.T, s *schema.Schema) DiffableSchema {
	objectDefs, _, err := s.ToDefinitions()
	require.NoError(t, err)

	definitions := make([]compiler.SchemaDefinition, 0, len(objectDefs))
	for _, objectDef := range objectDefs {
		definitions = append(definitions, objectDef)
	}

	schemaText, _, err := generator.GenerateSchema(t.Context(), definitions)
	require.NoError(t, err)

	compiled, err := compiler.Compile(
		compiler.InputSchema{Source: input.Source("schema"), SchemaString: schemaText},
		compiler.AllowUnprefixedObjectType(),
	)
	require.NoError(t, err)
	return NewDiffableSchemaFromCompiledSchema(compiled)
}

// expectedChangesFor returns the changes that a diff from the base of the variant to its
// schema is expected to report, or the reverse.
func expectedChangesFor(mutations []schematesting.Mutation, reversed bool) expectedChanges {
	added, removed := "added", "removed"
	if reversed {
		added, removed = removed, added
	}

	expected := expectedChanges{changedNamespaces: map[string][]string{}}
	addDelta := func(mutation schematesting.Mutation, deltaType string) {
		delta := deltaType + ":" + mutation.Name
		if len(mutation.AllowedTypes) == 1 && (mutation.Kind == schematesting.AddAllowedType || mutation.Kind == schematesting.RemoveAllowedType) {
			delta += ":" + mutation.AllowedTypes[0].Type + "#" + mutation.AllowedTypes[0].Subrelation
		}
		expected.changedNamespaces[mutation.Definition] = append(expected.changedNamespaces[mutation.Definition], delta)
	}

	for _, mutation := range mutations {
		switch mutation.Kind {
		case schematesting.AddDefinition:
			if reversed {
				expected.removedNamespaces = append(expected.removedNamespaces, mutation.Definition)
			} else {
				expected.addedNamespaces = append(expected.addedNamespaces, mutation.Definition)
			}

		case schematesting.RemoveDefinition:
			if reversed {
				expected.addedNamespaces = append(expected.addedNamespaces, mutation.Definition)
			} else {
				expected.removedNamespaces = append(expected.removedNamespaces, mutation.Definition)
			}

		case schematesting.AddRelation:
			addDelta(mutation, added+"-relation")

		case schematesting.RemoveRelation:
			addDelta(mutation, removed+"-relation")

		case schematesting.AddAllowedType:
			addDelta(mutation, "relation-allowed-type-"+added)

		case schematesting.RemoveAllowedType:
			addDelta(mutation, "relation-allowed-type-"+removed)

		case schematesting.AddPermission:
			addDelta(mutation, added+"-permission")

		case schematesting.RemovePermission:
			addDelta(mutation, removed+"-permission")

		case schematesting.ChangePermission, schematesting.AddUnionChild, schematesting.RemoveUnionChild:
			addDelta(mutation, string(namespace.ChangedPermissionImpl))
		}
	}

	slices.Sort(expected.addedNamespaces)
	slices.Sort(expected.removedNamespaces)
	for _, deltas := range expected.changedNamespaces {
		slices.Sort(deltas)
	}
	return expected
}

// foundChangesFor returns the changes reported by the diff, in the same form as the
// expected changes.
func foundChangesFor(schemaDiff *SchemaDiff) expectedChanges {
	found := expectedChanges{
		addedNamespaces:   slices.Sorted(slices.Values(schemaDiff.AddedNamespaces)),
		removedNamespaces: slices.Sorted(slices.Values(schemaDiff.RemovedNamespaces)),
		changedNamespaces: map[string][]string{},
	}

	for namespaceName, namespaceDiff := range schemaDiff.ChangedNamespaces {
		deltas := make([]string, 0, len(namespaceDiff.Deltas()))
		for _, delta := range namespaceDiff.Deltas() {
			formatted := fmt.Sprintf("%s:%s", delta.Type, delta.RelationName)
			if delta.AllowedType != nil {
				formatted += ":" + delta.AllowedType.Namespace + "#" + delta.AllowedType.GetRelation()
			}
			deltas = append(deltas, formatted)
		}
		slices.Sort(deltas)
		found.changedNamespaces[namespaceName] = deltas
	}
	return found
}

func requireDiff(t *rapid.T, existing, comparison DiffableSchema, expected expectedChanges) {
	schemaDiff, err := DiffSchemas(existing, comparison, caveattypes.Default.TypeSet)
	require.NoError(t, err)
	require.Empty(t, schemaDiff.AddedCaveats)
	require.Empty(t, schemaDiff.RemovedCaveats)
	require.Empty(t, schemaDiff.ChangedCaveats)
	require.Equal(t, expected, foundChangesFor(schemaDiff))
}

func TestDiffSchemasMatchesMutations(t *testing.T) {
	schematesting.CheckWithSchemaVariant(t, func(t *rapid.T, v schematesting.Variant, _ schematesting.RelationshipGenerator) {
		expected := expectedChangesFor(v.Mutations, false)
		requireDiff(t, diffableFromSchema(t, v.Base), diffableFromSchema(t, v.Schema), expected)
		requireDiff(t, diffableFromSchemaText(t, v.Base), diffableFromSchemaText(t, v.Schema), expected)
	})
}

func TestDiffSchemasSelfIsEmpty(t *testing.T) {
	schematesting.CheckWithSchemaVariant(t, func(t *rapid.T, v schematesting.Variant, _ schematesting.RelationshipGenerator) {
		empty := expectedChanges{changedNamespaces: map[string][]string{}}
		requireDiff(t, diffableFromSchema(t, v.Schema), diffableFromSchema(t, v.Schema), empty)
		requireDiff(t, diffableFromSchemaText(t, v.Schema), diffableFromSchemaText(t, v.Schema), empty)
	})
}

func TestDiffSchemasReverseMirrors(t *testing.T) {
	schematesting.CheckWithSchemaVariant(t, func(t *rapid.T, v schematesting.Variant, _ schematesting.RelationshipGenerator) {
		expected := expectedChangesFor(v.Mutations, true)
		requireDiff(t, diffableFromSchema(t, v.Schema), diffableFromSchema(t, v.Base), expected)
		requireDiff(t, diffableFromSchemaText(t, v.Schema), diffableFromSchemaText(t, v.Base), expected)

		// The inverse of the variant describes the same changes.
		inverse := v.Inverse()
		expected = expectedChangesFor(inverse.Mutations, false)
		requireDiff(t, diffableFromSchema(t, inverse.Base), diffableFromSchema(t, inverse.Schema), expected)
		requireDiff(t, diffableFromSchemaText(t, inverse.Base), diffableFromSchemaText(t, inverse.Schema), expected)
	})
}

// TestDiffSchemasOverTime ensures the diff also matches for a schema that has been changed
// multiple times.
func TestDiffSchemasOverTime(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		current, _ := schematesting.GenerateSchema(t)
		for range 3 {
			v := schematesting.GenerateVariant(t, current)
			requireDiff(t, diffableFromSchema(t, v.Base), diffableFromSchema(t, v.Schema), expectedChangesFor(v.Mutations, false))
			current = v.Schema
		}
	})
}
