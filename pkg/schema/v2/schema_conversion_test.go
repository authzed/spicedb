package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestSchemaConversionFromCompiler(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name                string
		schemaText          string
		expectedDefinitions []string
		expectedRelations   map[string][]string // definition -> relations
		expectedPermissions map[string][]string // definition -> permissions
		expectedCaveats     []string
	}

	tcs := []testcase{
		{
			name: "basic schema with relations and permissions",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				permission view = org->member + viewer
			}
		`,
			expectedDefinitions: []string{"user", "organization", "resource"},
			expectedRelations: map[string][]string{
				"organization": {"member"},
				"resource":     {"org", "viewer"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view"},
			},
			expectedCaveats: []string{},
		},
		{
			name: "complex schema with multiple operations",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
				relation admin: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				relation banned: user
				permission view = (org->member + viewer) - banned
				permission admin = org->admin
			}
		`,
			expectedDefinitions: []string{"user", "organization", "resource"},
			expectedRelations: map[string][]string{
				"organization": {"member", "admin"},
				"resource":     {"org", "viewer", "banned"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view", "admin"},
			},
			expectedCaveats: []string{},
		},
		{
			name: "schema with wildcards and multi-type relations",
			schemaText: `
			definition user {}
			definition team {}

			definition organization {
				relation member: user:*
				relation admin: user | team
			}

			definition resource {
				relation viewer: organization#member
				permission view = viewer
			}
		`,
			expectedDefinitions: []string{"user", "team", "organization", "resource"},
			expectedRelations: map[string][]string{
				"organization": {"member", "admin"},
				"resource":     {"viewer"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view"},
			},
			expectedCaveats: []string{},
		},
		{
			name: "schema with caveats",
			schemaText: `
			caveat must_be_ip_address(ip_address string) {
				ip_address == "127.0.0.1"
			}

			definition user {}

			definition resource {
				relation viewer: user with must_be_ip_address
				permission view = viewer
			}
		`,
			expectedDefinitions: []string{"user", "resource"},
			expectedRelations: map[string][]string{
				"resource": {"viewer"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view"},
			},
			expectedCaveats: []string{"must_be_ip_address"},
		},
		{
			name: "schema with intersection and exclusion",
			schemaText: `
			definition user {}

			definition organization {
				relation member: user
				relation admin: user
			}

			definition resource {
				relation org: organization
				relation viewer: user
				relation banned: user
				permission view = (org->member & viewer) - banned
			}
		`,
			expectedDefinitions: []string{"user", "organization", "resource"},
			expectedRelations: map[string][]string{
				"organization": {"member", "admin"},
				"resource":     {"org", "viewer", "banned"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view"},
			},
			expectedCaveats: []string{},
		},
		{
			name: "schema with multiple caveats",
			schemaText: `
			caveat ip_check(ip string) {
				ip == "192.168.1.1"
			}

			caveat time_check(current_time timestamp) {
				current_time < timestamp("2024-12-31T23:59:59Z")
			}

			definition user {}

			definition resource {
				relation viewer: user with ip_check
				relation editor: user with time_check
				permission view = viewer
				permission edit = editor
			}
		`,
			expectedDefinitions: []string{"user", "resource"},
			expectedRelations: map[string][]string{
				"resource": {"viewer", "editor"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view", "edit"},
			},
			expectedCaveats: []string{"ip_check", "time_check"},
		},
		{
			name: "schema with caveat containing complex expression",
			schemaText: `
			caveat complex_check(user_id string, is_admin bool, access_level int) {
				user_id != "" && (is_admin == true || access_level >= 5)
			}

			definition user {}

			definition document {
				relation owner: user
				relation restricted_viewer: user with complex_check
				permission view = owner + restricted_viewer
			}
		`,
			expectedDefinitions: []string{"user", "document"},
			expectedRelations: map[string][]string{
				"document": {"owner", "restricted_viewer"},
			},
			expectedPermissions: map[string][]string{
				"document": {"view"},
			},
			expectedCaveats: []string{"complex_check"},
		},
		{
			name: "schema with caveat on computed userset",
			schemaText: `
			caveat region_check(region string) {
				region == "us-east-1"
			}

			definition user {}

			definition organization {
				relation member: user
			}

			definition resource {
				relation org: organization
				relation direct_viewer: user with region_check
				permission view = org->member + direct_viewer
			}
		`,
			expectedDefinitions: []string{"user", "organization", "resource"},
			expectedRelations: map[string][]string{
				"organization": {"member"},
				"resource":     {"org", "direct_viewer"},
			},
			expectedPermissions: map[string][]string{
				"resource": {"view"},
			},
			expectedCaveats: []string{"region_check"},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			// Compile the schema using the existing compiler
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Build the v2 schema from the compiled schema
			v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
			require.NoError(t, err)

			// Test that we have the expected number of definitions
			require.Len(t, v2Schema.Definitions(), len(tc.expectedDefinitions))

			// Test that we have the expected number of caveats
			require.Len(t, v2Schema.Caveats(), len(tc.expectedCaveats))

			// Verify each expected definition exists
			for _, defName := range tc.expectedDefinitions {
				def, exists := v2Schema.Definitions()[defName]
				require.True(t, exists, "Definition %s should exist", defName)
				require.Equal(t, defName, def.Name())
				require.Equal(t, v2Schema, def.Parent())
			}

			// Verify each expected caveat exists
			for _, caveatName := range tc.expectedCaveats {
				caveat, exists := v2Schema.Caveats()[caveatName]
				require.True(t, exists, "Caveat %s should exist", caveatName)
				require.Equal(t, caveatName, caveat.Name())
				require.Equal(t, v2Schema, caveat.Parent())
			}

			// Verify relations for each definition
			for defName, expectedRelations := range tc.expectedRelations {
				def, exists := v2Schema.Definitions()[defName]
				require.True(t, exists, "Definition %s should exist", defName)

				require.Len(t, def.Relations(), len(expectedRelations))

				for _, relName := range expectedRelations {
					rel, exists := def.Relations()[relName]
					require.True(t, exists, "Relation %s should exist in definition %s", relName, defName)
					require.Equal(t, relName, rel.Name())
					require.Equal(t, def, rel.Parent())
					require.NotEmpty(t, rel.BaseRelations(), "Relation %s should have allowed types", relName)
				}
			}

			// Verify permissions for each definition
			for defName, expectedPermissions := range tc.expectedPermissions {
				def, exists := v2Schema.Definitions()[defName]
				require.True(t, exists, "Definition %s should exist", defName)

				require.Len(t, def.Permissions(), len(expectedPermissions))

				for _, permName := range expectedPermissions {
					perm, exists := def.Permissions()[permName]
					require.True(t, exists, "Permission %s should exist in definition %s", permName, defName)
					require.Equal(t, permName, perm.Name())
					require.Equal(t, def, perm.Parent())
					require.NotNil(t, perm.Operation(), "Permission %s should have an operation", permName)
				}
			}

			// Verify that the compiled schema and v2 schema have consistent data
			require.Equal(t, len(compiled.ObjectDefinitions), len(v2Schema.Definitions()))
			require.Equal(t, len(compiled.CaveatDefinitions), len(v2Schema.Caveats()))

			// Check that each compiled definition maps to a v2 definition
			for _, compiledDef := range compiled.ObjectDefinitions {
				v2Def, exists := v2Schema.Definitions()[compiledDef.Name]
				require.True(t, exists, "Compiled definition %s should exist in v2 schema", compiledDef.Name)
				require.Equal(t, compiledDef.Name, v2Def.Name())

				// Count relations and permissions from compiled schema
				compiledRelations := 0
				compiledPermissions := 0
				for _, relation := range compiledDef.Relation {
					if relation.GetTypeInformation() != nil {
						compiledRelations++
					}
					if relation.GetUsersetRewrite() != nil {
						compiledPermissions++
					}
				}

				require.Equal(t, compiledRelations, len(v2Def.Relations()))
				require.Equal(t, compiledPermissions, len(v2Def.Permissions()))
			}

			// Check that each compiled caveat maps to a v2 caveat
			for _, compiledCaveat := range compiled.CaveatDefinitions {
				v2Caveat, exists := v2Schema.Caveats()[compiledCaveat.Name]
				require.True(t, exists, "Compiled caveat %s should exist in v2 schema", compiledCaveat.Name)
				require.Equal(t, compiledCaveat.Name, v2Caveat.Name())
				require.Equal(t, string(compiledCaveat.SerializedExpression), v2Caveat.Expression())
			}
		})
	}
}

func TestSchemaConversionEdgeCases(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name        string
		schemaText  string
		expectError bool
	}

	tcs := []testcase{
		{
			name:        "empty schema",
			schemaText:  ``,
			expectError: false,
		},
		{
			name: "single definition only",
			schemaText: `
			definition user {}
		`,
			expectError: false,
		},
		{
			name: "caveat only",
			schemaText: `
			caveat simple_caveat(value string) {
				value == "test"
			}
		`,
			expectError: false,
		},
		{
			name: "relation without type information",
			schemaText: `
			definition user {}
			definition document {
				relation viewer: user
			}
		`,
			expectError: false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			// Compile the schema
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)

			// Build the v2 schema
			v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
			require.NoError(t, err)
			require.NotNil(t, v2Schema)

			// Basic validation that schema is well-formed
			require.NotNil(t, v2Schema.Definitions())
			require.NotNil(t, v2Schema.Caveats())

			// Verify Parent relationships are set correctly
			for _, def := range v2Schema.Definitions() {
				require.Equal(t, v2Schema, def.Parent())

				for _, rel := range def.Relations() {
					require.Equal(t, def, rel.Parent())
				}

				for _, perm := range def.Permissions() {
					require.Equal(t, def, perm.Parent())
				}
			}

			for _, caveat := range v2Schema.Caveats() {
				require.Equal(t, v2Schema, caveat.Parent())
			}
		})
	}
}

func TestSchemaConversionOperationTypes(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name         string
		schemaText   string
		definition   string
		permission   string
		expectedType string // "union", "intersection", "exclusion", "relation"
	}

	tcs := []testcase{
		{
			name: "union operation",
			schemaText: `
			definition user {}
			definition resource {
				relation viewer: user
				relation editor: user
				permission view = viewer + editor
			}
		`,
			definition:   "resource",
			permission:   "view",
			expectedType: "union",
		},
		{
			name: "intersection operation",
			schemaText: `
			definition user {}
			definition resource {
				relation viewer: user
				relation editor: user
				permission view = viewer & editor
			}
		`,
			definition:   "resource",
			permission:   "view",
			expectedType: "intersection",
		},
		{
			name: "exclusion operation",
			schemaText: `
			definition user {}
			definition resource {
				relation viewer: user
				relation banned: user
				permission view = viewer - banned
			}
		`,
			definition:   "resource",
			permission:   "view",
			expectedType: "exclusion",
		},
		{
			name: "simple relation reference",
			schemaText: `
			definition user {}
			definition resource {
				relation viewer: user
				permission view = viewer
			}
		`,
			definition:   "resource",
			permission:   "view",
			expectedType: "relation",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			// Compile and build schema
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
			require.NoError(t, err)

			// Find the permission
			def, exists := v2Schema.Definitions()[tc.definition]
			require.True(t, exists)

			perm, exists := def.Permissions()[tc.permission]
			require.True(t, exists)
			require.NotNil(t, perm.Operation())

			// Check operation type
			switch tc.expectedType {
			case "union":
				_, ok := perm.Operation().(*UnionOperation)
				require.True(t, ok, "Expected UnionOperation but got %T", perm.Operation())
			case "intersection":
				_, ok := perm.Operation().(*IntersectionOperation)
				require.True(t, ok, "Expected IntersectionOperation but got %T", perm.Operation())
			case "exclusion":
				_, ok := perm.Operation().(*ExclusionOperation)
				require.True(t, ok, "Expected ExclusionOperation but got %T", perm.Operation())
			case "relation":
				_, ok := perm.Operation().(*RelationReference)
				require.True(t, ok, "Expected RelationReference but got %T", perm.Operation())
			}
		})
	}
}
