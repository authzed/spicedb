package testing

import (
	"context"
	"slices"
	"strings"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// mustSchema compiles the schema text into a schema.
func mustSchema(t require.TestingT, text string) *schema.Schema {
	compiled, err := compiler.Compile(
		compiler.InputSchema{Source: input.Source("test"), SchemaString: text},
		compiler.AllowUnprefixedObjectType(),
	)
	require.NoError(t, err)

	s, err := schema.BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	return s
}

// schemaText renders the schema as normalized schema text.
func schemaText(t require.TestingT, s *schema.Schema) string {
	typeDefs, caveatDefs, err := s.ToDefinitions()
	require.NoError(t, err)

	// ToDefinitions iterates over maps, so sort to produce stable text.
	slices.SortFunc(typeDefs, func(a, b *core.NamespaceDefinition) int {
		return strings.Compare(a.Name, b.Name)
	})

	definitions := make([]compiler.SchemaDefinition, 0, len(typeDefs)+len(caveatDefs))
	for _, cd := range caveatDefs {
		definitions = append(definitions, cd)
	}
	for _, td := range typeDefs {
		slices.SortFunc(td.Relation, func(a, b *core.Relation) int {
			return strings.Compare(a.Name, b.Name)
		})
		definitions = append(definitions, td)
	}

	generated, _, err := generator.GenerateSchema(context.Background(), definitions)
	require.NoError(t, err)
	return generated
}
