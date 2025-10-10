package schema

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestFlattenSchema(t *testing.T) {
	tests := []struct {
		name           string
		schemaString   string
		expectedString string
	}{
		{
			name: "simple relation reference",
			schemaString: `definition document {
	relation viewer: user
	permission view = viewer
}

definition user {}`,
			expectedString: `definition document {
	relation viewer: user
	permission view = viewer
}

definition user {}`,
		},
		{
			name: "nested union in permission",
			schemaString: `definition document {
	relation viewer: user
	relation editor: user
	relation owner: user
	permission view = viewer + (editor + owner)
}

definition user {}`,
			// Note: The schema generator optimizes unions, so (a + b) + c becomes a + b + c
			// This is semantically equivalent but doesn't preserve the synthetic permission
			expectedString: `definition document {
	relation editor: user
	relation owner: user
	relation viewer: user
	permission view = viewer + editor + owner
}

definition user {}`,
		},
		{
			name: "nested intersection",
			schemaString: `definition document {
	relation viewer: user
	relation approved: user
	relation active: user
	permission view = viewer & (approved & active)
}

definition user {}`,
			// Note: The schema generator optimizes intersections, so (a & b) & c becomes a & b & c
			// This is semantically equivalent but doesn't preserve the synthetic permission
			expectedString: `definition document {
	relation active: user
	relation approved: user
	relation viewer: user
	permission view = viewer & approved & active
}

definition user {}`,
		},
		{
			name: "complex nested operation",
			schemaString: `definition document {
	relation viewer: user
	relation editor: user
	relation owner: user
	relation banned: user
	permission view = (viewer + editor) & owner - banned
}

definition user {}`,
			expectedString: `definition document {
	relation banned: user
	relation editor: user
	relation owner: user
	relation viewer: user
	permission view = view__2206507c0cd4deac - banned
	permission view__2206507c0cd4deac = view__68f54b6bc11ce517 & owner
	permission view__68f54b6bc11ce517 = viewer + editor
}

definition user {}`,
		},
		{
			name: "nested operations on both sides of intersection",
			schemaString: `definition document {
	relation alpha: user
	relation beta: user
	relation gamma: user
	permission view = (alpha + beta) & (beta - gamma)
}

definition user {}`,
			expectedString: `definition document {
	relation alpha: user
	relation beta: user
	relation gamma: user
	permission view = view__ca785c1f06ee59a8 & view__30cc7691c277d600
	permission view__30cc7691c277d600 = beta - gamma
	permission view__ca785c1f06ee59a8 = alpha + beta
}

definition user {}`,
		},
		{
			name: "deeply nested operation with multiple levels",
			schemaString: `definition document {
	relation alpha: user
	relation beta: user
	relation gamma: user
	relation delta: user
	permission view = ((alpha & beta) - gamma) & delta
}

definition user {}`,
			expectedString: `definition document {
	relation alpha: user
	relation beta: user
	relation delta: user
	relation gamma: user
	permission view = view__fa94e62ac301a8fe & delta
	permission view__0a74441a3356055f = alpha & beta
	permission view__fa94e62ac301a8fe = view__0a74441a3356055f - gamma
}

definition user {}`,
		},
		{
			name: "deeply nested with exclusion at top level",
			schemaString: `definition document {
	relation alpha: user
	relation beta: user
	relation gamma: user
	relation delta: user
	relation epsilon: user
	permission view = (((alpha & beta) - gamma) & delta) - epsilon
}

definition user {}`,
			expectedString: `definition document {
	relation alpha: user
	relation beta: user
	relation delta: user
	relation epsilon: user
	relation gamma: user
	permission view = view__43a23512cd075783 - epsilon
	permission view__0a74441a3356055f = alpha & beta
	permission view__43a23512cd075783 = view__fa94e62ac301a8fe & delta
	permission view__fa94e62ac301a8fe = view__0a74441a3356055f - gamma
}

definition user {}`,
		},
		{
			name: "multiple intersections with exclusion",
			schemaString: `definition document {
	relation alpha: user
	relation beta: user
	relation gamma: user
	relation delta: user
	relation epsilon: user
	relation omega: user
	permission view = ((alpha & beta & omega) - gamma) & delta & epsilon
}

definition user {}`,
			expectedString: `definition document {
	relation alpha: user
	relation beta: user
	relation delta: user
	relation epsilon: user
	relation gamma: user
	relation omega: user
	permission view = view__d8c31e0939daa3b2 & delta & epsilon
	permission view__27eaf9ef96df1dec = alpha & beta & omega
	permission view__d8c31e0939daa3b2 = view__27eaf9ef96df1dec - gamma
}

definition user {}`,
		},
		{
			name: "repeated subexpressions with different operations",
			schemaString: `definition document {
	relation alpha: user
	relation beta: user
	permission view = (alpha + beta) & (alpha + beta) & (alpha - beta)
}

definition user {}`,
			expectedString: `definition document {
	relation alpha: user
	relation beta: user
	permission view = view__ca785c1f06ee59a8 & view__ca785c1f06ee59a8 & view__f74d2059f9a5635f
	permission view__ca785c1f06ee59a8 = alpha + beta
	permission view__f74d2059f9a5635f = alpha - beta
}

definition user {}`,
		},
		{
			name: "arrow operations in intersection",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	permission view = (foo->bar) & (bar->baz)
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
			expectedString: `definition document {
	relation bar: folder
	relation foo: folder
	permission view = foo->bar & bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
		},
		{
			name: "nested arrow operations with exclusion",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	permission view = (foo->bar & bar->baz) - (foo->bar)
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
			expectedString: `definition document {
	relation bar: folder
	relation foo: folder
	permission view = view__70cbb44675052ab6 - foo->bar
	permission view__70cbb44675052ab6 = foo->bar & bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Step 1: Compile the schema
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tt.schemaString,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Step 2: Convert to *Schema
			schema, err := BuildSchemaFromCompiledSchema(*compiled)
			require.NoError(t, err)
			require.NotNil(t, schema)

			// Step 3: Resolve the schema
			resolved, err := ResolveSchema(schema)
			require.NoError(t, err)
			require.NotNil(t, resolved)

			// Step 4: Flatten the schema
			flattened, err := FlattenSchema(resolved, FlattenSeparatorDoubleUnderscore)
			require.NoError(t, err)
			require.NotNil(t, flattened)

			// Step 5: Convert back to corev1
			defs, caveats, err := flattened.ResolvedSchema().Schema().ToNamespaceDefinition("test")
			require.NoError(t, err)
			require.NotNil(t, defs)

			// Step 6: Generate schema string
			// Sort definitions by name for deterministic output
			sort.Slice(defs, func(i, j int) bool {
				return defs[i].Name < defs[j].Name
			})

			// Sort relations within each definition for deterministic output
			for _, def := range defs {
				sort.Slice(def.Relation, func(i, j int) bool {
					return def.Relation[i].Name < def.Relation[j].Name
				})
			}

			var schemaDefinitions []compiler.SchemaDefinition
			for _, def := range defs {
				schemaDefinitions = append(schemaDefinitions, def)
			}
			for _, caveat := range caveats {
				schemaDefinitions = append(schemaDefinitions, caveat)
			}

			generatedSchema, _, err := generator.GenerateSchema(schemaDefinitions)
			require.NoError(t, err)

			// Verify the generated schema compiles
			recompiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("regenerated"),
				SchemaString: generatedSchema,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)
			require.NotNil(t, recompiled)

			// Compare the generated schema with expected
			// We use a second compilation to normalize both schemas for comparison
			expectedCompiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("expected"),
				SchemaString: tt.expectedString,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Sort expected definitions and relations the same way
			sort.Slice(expectedCompiled.ObjectDefinitions, func(i, j int) bool {
				return expectedCompiled.ObjectDefinitions[i].Name < expectedCompiled.ObjectDefinitions[j].Name
			})

			for _, def := range expectedCompiled.ObjectDefinitions {
				sort.Slice(def.Relation, func(i, j int) bool {
					return def.Relation[i].Name < def.Relation[j].Name
				})
			}

			var expectedSchemaDefinitions []compiler.SchemaDefinition
			for _, def := range expectedCompiled.ObjectDefinitions {
				expectedSchemaDefinitions = append(expectedSchemaDefinitions, def)
			}
			for _, caveat := range expectedCompiled.CaveatDefinitions {
				expectedSchemaDefinitions = append(expectedSchemaDefinitions, caveat)
			}

			expectedGenerated, _, err := generator.GenerateSchema(expectedSchemaDefinitions)
			require.NoError(t, err)

			// Compare the two generated schemas
			require.Equal(t, expectedGenerated, generatedSchema, "Generated schema does not match expected")
		})
	}
}

func TestFlattenSchema_Nil(t *testing.T) {
	_, err := FlattenSchema(nil, FlattenSeparatorDoubleUnderscore)
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil")
}

func TestFlattenSchema_NoNesting(t *testing.T) {
	// Schema with no nested operations should remain unchanged (except for resolution)
	rel := &Relation{
		name:          "viewer",
		baseRelations: []*BaseRelation{},
	}

	perm := &Permission{
		name: "view",
		operation: &RelationReference{
			relationName: "viewer",
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	flattened, err := FlattenSchema(resolved, FlattenSeparatorDoubleUnderscore)
	require.NoError(t, err)
	require.NotNil(t, flattened)

	// Should have same number of permissions (no synthetic ones added)
	flattenedDef := flattened.ResolvedSchema().Schema().definitions["document"]
	require.Len(t, flattenedDef.permissions, 1)
	require.Contains(t, flattenedDef.permissions, "view")
}

func TestFlattenSchema_SimpleNesting(t *testing.T) {
	// Build: permission view = viewer + (editor + owner)
	rel1 := &Relation{name: "viewer", baseRelations: []*BaseRelation{}}
	rel2 := &Relation{name: "editor", baseRelations: []*BaseRelation{}}
	rel3 := &Relation{name: "owner", baseRelations: []*BaseRelation{}}

	perm := &Permission{
		name: "view",
		operation: &UnionOperation{
			children: []Operation{
				&RelationReference{relationName: "viewer"},
				&UnionOperation{
					children: []Operation{
						&RelationReference{relationName: "editor"},
						&RelationReference{relationName: "owner"},
					},
				},
			},
		},
	}

	def := &Definition{
		name: "document",
		relations: map[string]*Relation{
			"viewer": rel1,
			"editor": rel2,
			"owner":  rel3,
		},
		permissions: map[string]*Permission{
			"view": perm,
		},
	}
	rel1.parent = def
	rel2.parent = def
	rel3.parent = def
	perm.parent = def

	schema := &Schema{
		definitions: map[string]*Definition{
			"document": def,
		},
		caveats: make(map[string]*Caveat),
	}
	def.parent = schema

	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	flattened, err := FlattenSchema(resolved, FlattenSeparatorDoubleUnderscore)
	require.NoError(t, err)
	require.NotNil(t, flattened)

	// Should have 2 permissions: original + 1 synthetic
	flattenedDef := flattened.ResolvedSchema().Schema().definitions["document"]
	require.Len(t, flattenedDef.permissions, 2)
	require.Contains(t, flattenedDef.permissions, "view")

	// Find the synthetic permission
	var synthPermName string
	for name, p := range flattenedDef.permissions {
		if name != "view" {
			synthPermName = name
			require.True(t, p.IsSynthetic(), "expected synthetic permission")
		}
	}
	require.NotEmpty(t, synthPermName)
	require.Contains(t, synthPermName, "view__")

	// The view permission should now reference the synthetic permission
	viewPerm := flattenedDef.permissions["view"]
	union := viewPerm.operation.(*UnionOperation)
	require.Len(t, union.children, 2)

	// Second child should be a reference to the synthetic permission
	secondChild := union.children[1].(*ResolvedRelationReference)
	require.Equal(t, synthPermName, secondChild.relationName)
}
