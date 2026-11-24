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

func TestGenerateSchemaComposes(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		first, _ := GenerateSchema(t)
		second, _ := GenerateSchema(t)

		_, err := schema.ResolveSchema(first)
		require.NoError(t, err)
		_, err = schema.ResolveSchema(second)
		require.NoError(t, err)
	})
}

func TestNewRelationshipGeneratorHandWritten(t *testing.T) {
	s := mustSchema(t, `
		definition user {}

		definition doc {
			relation viewer: user
			permission view = viewer
		}`)

	relGenerator, err := NewRelationshipGenerator(s)
	require.NoError(t, err)

	rapid.Check(t, func(t *rapid.T) {
		counter := 0
		for relationship := range relGenerator.GenerateRelationships(t) {
			require.Equal(t, "doc", relationship.Resource.ObjectType)
			require.Equal(t, "viewer", relationship.Resource.Relation)
			require.Equal(t, "user", relationship.Subject.ObjectType)
			require.Equal(t, tuple.Ellipsis, relationship.Subject.Relation)
			counter++
			if counter >= 20 {
				break
			}
		}
	})
}

func TestNewRelationshipGeneratorRejectsUnsupported(t *testing.T) {
	tcs := []struct {
		name     string
		schema   string
		expected string
	}{
		{
			"caveat",
			`caveat somecaveat(x int) { x == 1 }
			definition user {}
			definition doc {
				relation viewer: user with somecaveat
			}`,
			"caveat",
		},
		{
			"wildcard",
			`definition user {}
			definition doc {
				relation viewer: user:*
			}`,
			"wildcard",
		},
		{
			"expiration",
			`use expiration
			definition user {}
			definition doc {
				relation viewer: user with expiration
			}`,
			"expiration",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRelationshipGenerator(mustSchema(t, tc.schema))
			require.ErrorContains(t, err, "unsupported schema feature")
			require.ErrorContains(t, err, tc.expected)
			require.ErrorContains(t, err, "doc#viewer")
		})
	}
}

func TestRelationshipGeneratorWithObjectIDs(t *testing.T) {
	objectIDs := []string{"first", "second", "third"}

	CheckWithSchema(t, func(t *rapid.T, schema *schema.Schema, relGenerator RelationshipGenerator) {
		counter := 0
		for relationship := range relGenerator.WithObjectIDs(objectIDs...).GenerateRelationships(t) {
			require.Contains(t, objectIDs, relationship.Resource.ObjectID)
			require.Contains(t, objectIDs, relationship.Subject.ObjectID)
			counter++
			if counter >= 20 {
				break
			}
		}
	})
}

func TestNewRelationshipGeneratorNoResources(t *testing.T) {
	_, err := NewRelationshipGenerator(mustSchema(t, `definition user {}`))
	require.ErrorContains(t, err, "no relations")
}
