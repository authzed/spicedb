package testing

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/internal/relationships"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	pkgschema "github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestScenariosAreValid(t *testing.T) {
	shared := Direct()
	for _, tc := range []struct {
		name  string
		shape *Shape
	}{
		{"direct", Direct()},
		{"empty", Empty()},
		{"alias", Alias(Alias(Direct()))},
		{"userset", Userset(Intersection(Direct(), Direct()))},
		{"arrow", Arrow(Union(Direct(), Direct()))},
		{"all_arrow", AllArrow(Exclusion(Direct(), Direct()))},
		{"nested", Exclusion(Union(Direct(), Arrow(Userset(Direct()))), Direct())},
		{"shared", Union(Arrow(shared), Arrow(shared), Alias(shared))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				checkScenarioValidity(t, DrawScenario(t, tc.shape))
			})
		})
	}
}

func TestRandomScenariosAreValid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		checkScenarioValidity(t, DrawScenario(t, DrawShape(t, 3)))
	})
}

func TestLegacyGeneratedSchemasAreValid(t *testing.T) {
	CheckWithSchema(t, func(t *rapid.T, generated *schema.Schema, generator RelationshipGenerator) {
		scenario := Scenario{Schema: generated}
		for name, def := range generated.Definitions() {
			for permission := range def.Permissions() {
				scenario.Resources = append(scenario.Resources, tuple.ObjectAndRelation{ObjectType: name, ObjectID: "example", Relation: permission})
			}
		}
		seen := map[tuple.RelationshipReference]bool{}
		count := 0
		for rel := range generator.GenerateRelationships(t) {
			if !seen[rel.RelationshipReference] {
				scenario.Relationships = append(scenario.Relationships, rel)
				seen[rel.RelationshipReference] = true
			}
			scenario.Subjects = append(scenario.Subjects, rel.Subject)
			count++
			if count == 30 {
				break
			}
		}
		checkScenarioValidity(t, scenario)
	})
}

// This catches invalid schema combinations, writes to permissions, dangling
// usersets, duplicate identities, and CREATEs that collide after earlier batches.
func checkScenarioValidity(t *rapid.T, scenario Scenario) {
	defs, caveats, err := scenario.Schema.ToDefinitions()
	require.NoError(t, err)
	ts := pkgschema.NewTypeSystem(pkgschema.ResolverForSchema(&compiler.CompiledSchema{
		ObjectDefinitions: defs,
		CaveatDefinitions: caveats,
	}))
	namespaces := map[string]*pkgschema.Definition{}
	for _, def := range defs {
		require.NoError(t, protovalidate.Validate(def))
		_, err := ts.GetValidatedDefinition(t.Context(), def.Name)
		require.NoError(t, err)
		namespaces[def.Name], err = pkgschema.NewDefinition(def)
		require.NoError(t, err)
	}
	_, err = schema.ResolveSchema(scenario.Schema)
	require.NoError(t, err)

	validate := func(rel tuple.Relationship) {
		require.NoError(t, relationships.ValidateOneRelationship(namespaces, nil,
			caveattypes.Default.TypeSet, rel, relationships.ValidateRelationshipForCreateOrTouch))
	}
	state := map[tuple.RelationshipReference]bool{}
	for _, rel := range scenario.Relationships {
		validate(rel)
		require.False(t, state[rel.RelationshipReference], "duplicate initial relationship: %s", rel)
		state[rel.RelationshipReference] = true
	}
	for _, batch := range scenario.UpdateBatches {
		require.NotEmpty(t, batch)
		seen := map[tuple.RelationshipReference]bool{}
		for _, update := range batch {
			validate(update.Relationship)
			key := update.Relationship.RelationshipReference
			require.False(t, seen[key], "duplicate identity within a batch")
			seen[key] = true
			switch update.Operation {
			case tuple.UpdateOperationCreate:
				require.False(t, state[key], "CREATE must refer to an absent relationship")
				state[key] = true
			case tuple.UpdateOperationTouch:
				state[key] = true
			case tuple.UpdateOperationDelete:
				delete(state, key)
			default:
				t.Fatalf("unknown update operation: %v", update.Operation)
			}
		}
	}
	require.NotEmpty(t, scenario.Resources)
	require.NotEmpty(t, scenario.Subjects)
	for _, resource := range scenario.Resources {
		require.True(t, namespaces[resource.ObjectType].IsPermission(resource.Relation))
	}
}

func TestScenarioUpdateBatchesRestoreInitialState(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		scenario := DrawScenario(t, Union(Direct(), Arrow(Direct())))
		require.GreaterOrEqual(t, len(scenario.UpdateBatches), 4)
		initial := map[tuple.RelationshipReference]bool{}
		for _, rel := range scenario.Relationships {
			initial[rel.RelationshipReference] = true
		}
		state := map[tuple.RelationshipReference]bool{}
		for key := range initial {
			state[key] = true
		}
		for i, batch := range scenario.UpdateBatches[:4] {
			for _, update := range batch {
				if update.Operation == tuple.UpdateOperationDelete {
					delete(state, update.Relationship.RelationshipReference)
				} else {
					state[update.Relationship.RelationshipReference] = true
				}
			}
			if i == 1 {
				require.Less(t, len(state), len(initial), "the deletion must change state")
			}
		}
		require.Equal(t, initial, state, "the transition prefix must restore the initial graph")
	})
}

func TestScenarioSharesReusedShapes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		shared := Direct()
		scenario := DrawScenario(t, Union(Arrow(shared), Arrow(shared)))
		// Both arrow relations must point to the same backing definition, so
		// changes to its membership affect both paths.
		root, ok := scenario.Schema.GetTypeDefinition(scenario.Resources[0].ObjectType)
		require.True(t, ok)
		require.Len(t, root.Relations(), 2)
		targets := map[string]bool{}
		for _, relation := range root.Relations() {
			for _, base := range relation.BaseRelations() {
				targets[base.Type()] = true
			}
		}
		require.Len(t, targets, 1)
	})
}

func TestShapeArguments(t *testing.T) {
	require.Panics(t, func() { Union() })
	require.Panics(t, func() { Intersection() })
	require.Panics(t, func() { Arrow(nil) })
	require.Panics(t, func() { Exclusion(Direct(), nil) })
}
