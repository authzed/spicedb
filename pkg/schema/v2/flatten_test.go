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
	permission view = view__b6febda281a9a6ff & view__159d4d2471796c72
	permission view__159d4d2471796c72 = bar->baz
	permission view__b6febda281a9a6ff = foo->bar
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
	permission view = view__6035022e4f2a427e - view__b6febda281a9a6ff
	permission view__159d4d2471796c72 = bar->baz
	permission view__6035022e4f2a427e = view__b6febda281a9a6ff & view__159d4d2471796c72
	permission view__b6febda281a9a6ff = foo->bar
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
			defs, caveats, err := flattened.ResolvedSchema().Schema().ToDefinitions()
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

func TestFlattenSchemaWithOptions_FlattenArrows(t *testing.T) {
	tests := []struct {
		name           string
		schemaString   string
		expectedString string
	}{
		{
			name: "arrow operations flattened in union",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	permission view = foo->bar + bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
			expectedString: `definition document {
	relation bar: folder
	relation foo: folder
	permission view = view__b6febda281a9a6ff + view__159d4d2471796c72
	permission view__159d4d2471796c72 = bar->baz
	permission view__b6febda281a9a6ff = foo->bar
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
		},
		{
			name: "arrow operations flattened in intersection",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	permission view = foo->bar & bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
			expectedString: `definition document {
	relation bar: folder
	relation foo: folder
	permission view = view__b6febda281a9a6ff & view__159d4d2471796c72
	permission view__159d4d2471796c72 = bar->baz
	permission view__b6febda281a9a6ff = foo->bar
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
		},
		{
			name: "arrow operations flattened in exclusion",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	permission view = foo->bar - bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
			expectedString: `definition document {
	relation bar: folder
	relation foo: folder
	permission view = view__b6febda281a9a6ff - view__159d4d2471796c72
	permission view__159d4d2471796c72 = bar->baz
	permission view__b6febda281a9a6ff = foo->bar
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
		},
		{
			name: "complex nested with arrows flattened",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	relation viewer: user
	permission view = (foo->bar & viewer) + bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
			expectedString: `definition document {
	relation bar: folder
	relation foo: folder
	relation viewer: user
	permission view = view__3440d68e2a434804 + view__159d4d2471796c72
	permission view__159d4d2471796c72 = bar->baz
	permission view__3440d68e2a434804 = view__b6febda281a9a6ff & viewer
	permission view__b6febda281a9a6ff = foo->bar
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
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

			// Step 4: Flatten the schema with FlattenArrows enabled
			flattened, err := FlattenSchemaWithOptions(resolved, FlattenOptions{
				Separator:                 FlattenSeparatorDoubleUnderscore,
				FlattenNonUnionOperations: true,
				FlattenArrows:             true,
			})
			require.NoError(t, err)
			require.NotNil(t, flattened)

			// Step 5: Convert back to corev1
			defs, caveats, err := flattened.ResolvedSchema().Schema().ToDefinitions()
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

func TestFlattenSchemaWithOptions_NoFlattenNonUnionOperations(t *testing.T) {
	schemaString := `definition document {
	relation viewer: user
	relation editor: user
	relation owner: user
	permission view = (viewer + editor) & owner
}

definition user {}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)
	require.NotNil(t, resolved)

	// Flatten the schema with FlattenNonUnionOperations disabled
	flattened, err := FlattenSchemaWithOptions(resolved, FlattenOptions{
		Separator:                 FlattenSeparatorDoubleUnderscore,
		FlattenNonUnionOperations: false,
		FlattenArrows:             false,
	})
	require.NoError(t, err)
	require.NotNil(t, flattened)

	// When FlattenNonUnionOperations is false, nested operations (including nested unions inside
	// intersections) should NOT be extracted. Only the original permission should remain.
	flattenedDef := flattened.ResolvedSchema().Schema().definitions["document"]
	require.Len(t, flattenedDef.permissions, 1)

	// The view permission should still be an intersection
	viewPerm := flattenedDef.permissions["view"]
	_, ok := viewPerm.operation.(*IntersectionOperation)
	require.True(t, ok, "expected top-level intersection to remain")
}

func TestFlattenSchemaWithOptions_Combinations(t *testing.T) {
	tests := []struct {
		name                      string
		schemaString              string
		flattenNonUnionOperations bool
		flattenArrows             bool
		expectedString            string
	}{
		{
			name: "flatten both non-union ops and arrows",
			schemaString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = (foo->bar & viewer) - foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = view__3440d68e2a434804 - view__b719eda281c0c047
	permission view__3440d68e2a434804 = view__b6febda281a9a6ff & viewer
	permission view__b6febda281a9a6ff = foo->bar
	permission view__b719eda281c0c047 = foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
		},
		{
			name: "flatten non-union ops only (not arrows)",
			schemaString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = (foo->bar & viewer) - foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             false,
			expectedString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = view__65817a3f96207a61 - foo->baz
	permission view__65817a3f96207a61 = foo->bar & viewer
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
		},
		{
			name: "flatten arrows only (not non-union ops)",
			schemaString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = (foo->bar & viewer) - foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
			flattenNonUnionOperations: false,
			flattenArrows:             true,
			expectedString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = (view__b6febda281a9a6ff & viewer) - view__b719eda281c0c047
	permission view__b6febda281a9a6ff = foo->bar
	permission view__b719eda281c0c047 = foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
		},
		{
			name: "flatten nothing (both options false)",
			schemaString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = (foo->bar & viewer) - foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
			flattenNonUnionOperations: false,
			flattenArrows:             false,
			expectedString: `definition document {
	relation foo: folder
	relation viewer: user
	permission view = (foo->bar & viewer) - foo->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}

definition user {}`,
		},
		{
			name: "complex with unions, intersections, and arrows - flatten all",
			schemaString: `definition document {
	relation alpha: folder
	relation beta: folder
	relation gamma: user
	permission view = ((alpha->foo + beta->bar) & gamma) - (alpha->baz)
}

definition folder {
	relation foo: folder
	permission bar = foo
	permission baz = foo
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedString: `definition document {
	relation alpha: folder
	relation beta: folder
	relation gamma: user
	permission view = view__fba7d0a3b79bde21 - view__723235cbec1b4a8b
	permission view__723235cbec1b4a8b = alpha->baz
	permission view__a7014dcc35ec916f = beta->bar
	permission view__b05635a82a557ee4 = alpha->foo
	permission view__eeff390875360d87 = view__b05635a82a557ee4 + view__a7014dcc35ec916f
	permission view__fba7d0a3b79bde21 = view__eeff390875360d87 & gamma
}

definition folder {
	relation foo: folder
	permission bar = foo
	permission baz = foo
}

definition user {}`,
		},
		{
			name: "complex with unions, intersections, and arrows - flatten non-union ops only",
			schemaString: `definition document {
	relation alpha: folder
	relation beta: folder
	relation gamma: user
	permission view = ((alpha->foo + beta->bar) & gamma) - (alpha->baz)
}

definition folder {
	relation foo: folder
	permission bar = foo
	permission baz = foo
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             false,
			expectedString: `definition document {
	relation alpha: folder
	relation beta: folder
	relation gamma: user
	permission view = view__ad6d6d250c6b1d43 - alpha->baz
	permission view__878eacc4f55f355f = alpha->foo + beta->bar
	permission view__ad6d6d250c6b1d43 = view__878eacc4f55f355f & gamma
}

definition folder {
	relation foo: folder
	permission bar = foo
	permission baz = foo
}

definition user {}`,
		},
		{
			name: "union with nil",
			schemaString: `definition document {
	relation viewer: user
	permission view = viewer + nil
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedString: `definition document {
	relation viewer: user
	permission view = viewer + nil
}

definition user {}`,
		},
		{
			name: "union with arrow and nil",
			schemaString: `definition organization {
	relation member: user
}
definition document {
	relation org: organization
	permission view = org->member + nil
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedString: `definition organization {
	relation member: user
}
definition document {
	relation org: organization
    permission view = view__c1dc49f4d1e680d0 + nil
    permission view__c1dc49f4d1e680d0 = org->member
}

definition user {}`,
		},
		{
			name: "multi-flatten with arrows and non-union ops",
			schemaString: `definition user {}

    definition document {
    	relation owner: user
    	relation editor: user
    	relation parent: folder
    	relation viewer: user

        permission edit = editor + owner
        permission view = viewer + edit + parent->view
        permission view_and_edit = view & edit
    }

    definition folder {
    	relation parent: folder
    	relation owner: user
    	relation editor: user
    	relation viewer: user | folder#view

        permission view = viewer + editor + owner + parent->view
    }`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedString: `definition document {
	permission edit = editor + owner
	relation editor: user
	relation owner: user
	relation parent: folder
	permission view = viewer + edit + view__0b0ed894546431d9
	permission view__0b0ed894546431d9 = parent->view
	permission view_and_edit = view & edit
	relation viewer: user
}
	
definition folder {
	relation editor: user
	relation owner: user
	relation parent: folder
	permission view = viewer + editor + owner + view__0b0ed894546431d9
	permission view__0b0ed894546431d9 = parent->view
	relation viewer: user | folder#view
}

definition user {}
`,
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

			// Step 4: Flatten the schema with specified options
			flattened, err := FlattenSchemaWithOptions(resolved, FlattenOptions{
				Separator:                 FlattenSeparatorDoubleUnderscore,
				FlattenNonUnionOperations: tt.flattenNonUnionOperations,
				FlattenArrows:             tt.flattenArrows,
			})
			require.NoError(t, err)
			require.NotNil(t, flattened)

			// Step 5: Convert back to corev1
			defs, caveats, err := flattened.ResolvedSchema().Schema().ToDefinitions()
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

func TestFlattenSchema_AnyFunctionedArrowEqualsRegularArrow(t *testing.T) {
	// Test that parent.any(viewer) is treated the same as parent->viewer for BDD hash purposes
	schemaWithAny := `definition document {
	relation parent: folder
	permission view = parent.any(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`

	schemaWithArrow := `definition document {
	relation parent: folder
	permission view = parent->viewer
}

definition folder {
	relation viewer: user
}

definition user {}`

	// Compile and resolve both schemas
	compiledAny, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaWithAny,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schemaAny, err := BuildSchemaFromCompiledSchema(*compiledAny)
	require.NoError(t, err)

	resolvedAny, err := ResolveSchema(schemaAny)
	require.NoError(t, err)

	compiledArrow, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaWithArrow,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schemaArrow, err := BuildSchemaFromCompiledSchema(*compiledArrow)
	require.NoError(t, err)

	resolvedArrow, err := ResolveSchema(schemaArrow)
	require.NoError(t, err)

	// Get the operations
	docDefAny := resolvedAny.Schema().definitions["document"]
	viewPermAny := docDefAny.permissions["view"]

	docDefArrow := resolvedArrow.Schema().definitions["document"]
	viewPermArrow := docDefArrow.permissions["view"]

	// Compute hashes for both operations
	hashAny, err := computeOperationHash(viewPermAny.operation)
	require.NoError(t, err)

	hashArrow, err := computeOperationHash(viewPermArrow.operation)
	require.NoError(t, err)

	// They should have the same hash because parent.any(viewer) == parent->viewer
	require.Equal(t, hashArrow, hashAny, "parent.any(viewer) should have same hash as parent->viewer")
}

func TestFlattenSchema_AllFunctionedArrowDiffersFromRegularArrow(t *testing.T) {
	// Test that parent.all(viewer) is treated differently from parent->viewer for BDD hash purposes
	schemaWithAll := `definition document {
	relation parent: folder
	permission view = parent.all(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`

	schemaWithArrow := `definition document {
	relation parent: folder
	permission view = parent->viewer
}

definition folder {
	relation viewer: user
}

definition user {}`

	// Compile and resolve both schemas
	compiledAll, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaWithAll,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schemaAll, err := BuildSchemaFromCompiledSchema(*compiledAll)
	require.NoError(t, err)

	resolvedAll, err := ResolveSchema(schemaAll)
	require.NoError(t, err)

	compiledArrow, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaWithArrow,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schemaArrow, err := BuildSchemaFromCompiledSchema(*compiledArrow)
	require.NoError(t, err)

	resolvedArrow, err := ResolveSchema(schemaArrow)
	require.NoError(t, err)

	// Get the operations
	docDefAll := resolvedAll.Schema().definitions["document"]
	viewPermAll := docDefAll.permissions["view"]

	docDefArrow := resolvedArrow.Schema().definitions["document"]
	viewPermArrow := docDefArrow.permissions["view"]

	// Compute hashes for both operations
	hashAll, err := computeOperationHash(viewPermAll.operation)
	require.NoError(t, err)

	hashArrow, err := computeOperationHash(viewPermArrow.operation)
	require.NoError(t, err)

	// They should have different hashes because parent.all(viewer) != parent->viewer
	require.NotEqual(t, hashArrow, hashAll, "parent.all(viewer) should have different hash from parent->viewer")
}

func TestFlattenSchema_AllFunctionedArrowDiffersFromAnyFunctionedArrow(t *testing.T) {
	// Test that parent.all(viewer) is treated differently from parent.any(viewer) for BDD hash purposes
	schemaWithAll := `definition document {
	relation parent: folder
	permission view = parent.all(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`

	schemaWithAny := `definition document {
	relation parent: folder
	permission view = parent.any(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`

	// Compile and resolve both schemas
	compiledAll, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaWithAll,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schemaAll, err := BuildSchemaFromCompiledSchema(*compiledAll)
	require.NoError(t, err)

	resolvedAll, err := ResolveSchema(schemaAll)
	require.NoError(t, err)

	compiledAny, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaWithAny,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	schemaAny, err := BuildSchemaFromCompiledSchema(*compiledAny)
	require.NoError(t, err)

	resolvedAny, err := ResolveSchema(schemaAny)
	require.NoError(t, err)

	// Get the operations
	docDefAll := resolvedAll.Schema().definitions["document"]
	viewPermAll := docDefAll.permissions["view"]

	docDefAny := resolvedAny.Schema().definitions["document"]
	viewPermAny := docDefAny.permissions["view"]

	// Compute hashes for both operations
	hashAll, err := computeOperationHash(viewPermAll.operation)
	require.NoError(t, err)

	hashAny, err := computeOperationHash(viewPermAny.operation)
	require.NoError(t, err)

	// They should have different hashes because parent.all(viewer) != parent.any(viewer)
	require.NotEqual(t, hashAny, hashAll, "parent.all(viewer) should have different hash from parent.any(viewer)")
}

func TestFlattenSchema_FunctionedArrowReference(t *testing.T) {
	tests := []struct {
		name                      string
		schemaString              string
		flattenNonUnionOperations bool
		flattenArrows             bool
		expectedPermissionCount   int // Expected number of permissions after flattening
	}{
		{
			name: "functioned arrow with flatten arrows enabled (in compound operation)",
			schemaString: `definition document {
	relation parent: folder
	relation viewer: user
	permission view = parent.any(viewer) + viewer
}

definition folder {
	relation viewer: user
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedPermissionCount:   2, // view + synthetic permission for functioned arrow
		},
		{
			name: "functioned arrow with flatten arrows disabled",
			schemaString: `definition document {
	relation parent: folder
	permission view = parent.any(viewer)
}

definition folder {
	relation viewer: user
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             false,
			expectedPermissionCount:   1, // just view
		},
		{
			name: "multiple functioned arrows with flatten arrows enabled",
			schemaString: `definition document {
	relation parent: folder
	relation owner: folder
	permission view = parent.any(viewer) + owner.all(editor)
}

definition folder {
	relation viewer: user
	relation editor: user
}

definition user {}`,
			flattenNonUnionOperations: true,
			flattenArrows:             true,
			expectedPermissionCount:   3, // view + 2 synthetic permissions
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

			// Step 4: Flatten the schema with specified options
			flattened, err := FlattenSchemaWithOptions(resolved, FlattenOptions{
				Separator:                 FlattenSeparatorDoubleUnderscore,
				FlattenNonUnionOperations: tt.flattenNonUnionOperations,
				FlattenArrows:             tt.flattenArrows,
			})
			require.NoError(t, err)
			require.NotNil(t, flattened)

			// Verify the expected number of permissions
			flattenedDef := flattened.ResolvedSchema().Schema().definitions["document"]
			require.Len(t, flattenedDef.permissions, tt.expectedPermissionCount)
		})
	}
}

func TestFlattenSchema_ResolvedReferences(t *testing.T) {
	tests := []struct {
		name         string
		schemaString string
	}{
		{
			name: "nested operations create resolved references",
			schemaString: `definition document {
	relation viewer: user
	relation editor: user
	relation owner: user
	permission view = (viewer + editor) & owner
}

definition user {}`,
		},
		{
			name: "deeply nested operations",
			schemaString: `definition document {
	relation alpha: user
	relation beta: user
	relation gamma: user
	relation delta: user
	permission view = ((alpha & beta) - gamma) & delta
}

definition user {}`,
		},
		{
			name: "arrow operations flattened",
			schemaString: `definition document {
	relation foo: folder
	relation bar: folder
	permission view = foo->bar & bar->baz
}

definition folder {
	relation bar: folder
	permission baz = bar
}`,
		},
		{
			name: "functioned arrow operations",
			schemaString: `definition document {
	relation parent: folder
	relation viewer: user
	permission view = parent.any(viewer) + viewer
}

definition folder {
	relation viewer: user
}

definition user {}`,
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

			// Step 5: Verify all ResolvedRelationReferences point to valid permissions
			flattenedSchema := flattened.ResolvedSchema().Schema()
			for _, def := range flattenedSchema.definitions {
				for _, perm := range def.permissions {
					verifyResolvedReferencesInOperation(t, perm.operation, def)
				}
			}
		})
	}
}

// verifyResolvedReferencesInOperation recursively checks that all ResolvedRelationReferences
// point to valid relations or permissions in the definition.
func verifyResolvedReferencesInOperation(t *testing.T, op Operation, def *Definition) {
	if op == nil {
		return
	}

	switch o := op.(type) {
	case *ResolvedRelationReference:
		// Verify the resolved field points to either a relation or permission
		require.NotNil(t, o.resolved, "ResolvedRelationReference.resolved should not be nil for %s", o.relationName)

		// Check if it's a relation
		if rel, ok := o.resolved.(*Relation); ok {
			// Verify the relation exists in the definition
			foundRel, exists := def.relations[o.relationName]
			require.True(t, exists, "Relation %s should exist in definition %s", o.relationName, def.name)
			// Verify they have the same name (pointer equality won't work after cloning)
			require.Equal(t, foundRel.name, rel.name, "Resolved relation should have same name")
			require.Equal(t, foundRel.parent, rel.parent, "Resolved relation should have same parent")
		} else if perm, ok := o.resolved.(*Permission); ok {
			// Verify the permission exists in the definition
			foundPerm, exists := def.permissions[o.relationName]
			require.True(t, exists, "Permission %s should exist in definition %s", o.relationName, def.name)
			// Verify they have the same name (pointer equality won't work after cloning)
			require.Equal(t, foundPerm.name, perm.name, "Resolved permission should have same name")
			require.Equal(t, foundPerm.parent, perm.parent, "Resolved permission should have same parent")
			// Most importantly, verify that the resolved field points to the exact same permission
			// object that's in the definition map (same pointer)
			require.Same(t, foundPerm, o.resolved, "ResolvedRelationReference should point to the exact permission object in the definition")
		} else {
			t.Fatalf("ResolvedRelationReference.resolved should be either *Relation or *Permission, got %T", o.resolved)
		}

	case *UnionOperation:
		for _, child := range o.children {
			verifyResolvedReferencesInOperation(t, child, def)
		}

	case *IntersectionOperation:
		for _, child := range o.children {
			verifyResolvedReferencesInOperation(t, child, def)
		}

	case *ExclusionOperation:
		verifyResolvedReferencesInOperation(t, o.left, def)
		verifyResolvedReferencesInOperation(t, o.right, def)

	case *ResolvedArrowReference, *ResolvedFunctionedArrowReference, *RelationReference, *ArrowReference, *FunctionedArrowReference, *NilReference:
		// These are leaf nodes, no further verification needed
		return

	default:
		t.Fatalf("Unknown operation type: %T", op)
	}
}

func TestFlattenSchema_SyntheticPermissionsAreResolved(t *testing.T) {
	schemaString := `definition document {
	relation viewer: user
	relation editor: user
	relation owner: user
	permission view = (viewer + editor) & owner
}

definition user {}`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaString,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to *Schema
	schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema
	resolved, err := ResolveSchema(schema)
	require.NoError(t, err)

	// Flatten the schema
	flattened, err := FlattenSchema(resolved, FlattenSeparatorDoubleUnderscore)
	require.NoError(t, err)

	// Get the flattened definition
	flattenedDef := flattened.ResolvedSchema().Schema().definitions["document"]

	// Find the view permission
	viewPerm, exists := flattenedDef.permissions["view"]
	require.True(t, exists)

	// The view permission should have an intersection operation
	intersection, ok := viewPerm.operation.(*IntersectionOperation)
	require.True(t, ok, "view permission should be an intersection")

	// The first child should be a resolved reference to a synthetic permission
	firstChild, ok := intersection.children[0].(*ResolvedRelationReference)
	require.True(t, ok, "first child should be a ResolvedRelationReference")

	// Verify the synthetic permission exists
	synthPerm, exists := flattenedDef.permissions[firstChild.relationName]
	require.True(t, exists, "synthetic permission %s should exist", firstChild.relationName)
	require.True(t, synthPerm.IsSynthetic(), "permission should be marked as synthetic")

	// Verify the resolved field points to the correct synthetic permission
	require.Equal(t, synthPerm, firstChild.resolved, "ResolvedRelationReference should point to the synthetic permission")
}
