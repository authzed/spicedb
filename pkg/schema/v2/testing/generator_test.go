package testing

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestExampleRunWithSchemaForTesting(t *testing.T) {
	CheckWithSchema(t, func(t *rapid.T, schema *schema.Schema, relGenerator RelationshipGenerator) {
		require.NotNil(t, schema)

		typeDefs, caveatDefs, err := schema.ToDefinitions()
		require.NoError(t, err)

		definitions := make([]compiler.SchemaDefinition, 0, len(typeDefs)+len(caveatDefs))
		for _, td := range typeDefs {
			require.NoError(t, protovalidate.Validate(td))
			definitions = append(definitions, td)
		}
		for _, cd := range caveatDefs {
			require.NoError(t, protovalidate.Validate(cd))
			definitions = append(definitions, cd)
		}

		generated, _, err := generator.GenerateSchema(t.Context(), definitions)
		require.NoError(t, err)
		t.Logf("Generated schema:\n%s", generated)

		counter := 0
		for relationship := range relGenerator.GenerateRelationships(t) {
			t.Logf("Generated relationship: %s\n", relationship.String())
			counter++
			if counter >= 5 {
				break
			}
		}
	})
}

func TestGenerateRelationshipsConformsToRegex(t *testing.T) {
	CheckWithSchema(t, func(t *rapid.T, schema *schema.Schema, relGenerator RelationshipGenerator) {
		require.NotNil(t, schema)

		counter := 0
		total := 1000
		for relationship := range relGenerator.GenerateRelationships(t) {
			_ = tuple.MustParse(relationship.String())
			counter++
			if counter >= total {
				break
			}
		}
	})
}

func TestGenerateRelationshipsPopulatesBackingRelations(t *testing.T) {
	built := schema.NewSchemaBuilder().
		AddDefinition("user").Done().
		AddDefinition("group").AddRelation("member").AllowedDirectRelation("user").Done().Done().
		AddDefinition("document").AddRelation("viewer").AllowedRelation("group", "member").Done().Done().Build()
	resolved, err := schema.ResolveSchema(built)
	require.NoError(t, err)
	rapid.Check(t, func(t *rapid.T) {
		generator := RelationshipGenerator{schema: resolved}
		types := map[string]bool{}
		ids := map[string]bool{}
		count := 0
		for rel := range generator.GenerateRelationships(t) {
			types[rel.Resource.ObjectType] = true
			ids[rel.Resource.ObjectID] = true
			ids[rel.Subject.ObjectID] = true
			count++
			if count == 2 {
				require.True(t, types["document"])
				require.True(t, types["group"], "the stream must populate userset backing relations")
			}
			if count == 100 {
				break
			}
		}
		require.LessOrEqual(t, len(ids), 4, "a small shared ID pool should connect the graph")
	})
}
