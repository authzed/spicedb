package testing

import (
	"testing"

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
			require.NoError(t, td.Validate())
			definitions = append(definitions, td)
		}
		for _, cd := range caveatDefs {
			require.NoError(t, cd.Validate())
			definitions = append(definitions, cd)
		}

		generated, _, err := generator.GenerateSchema(definitions)
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
