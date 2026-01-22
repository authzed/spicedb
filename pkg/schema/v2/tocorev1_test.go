package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestAsCompiledSchema(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name                    string
		schemaText              string
		expectedNumDefinitions  int
		expectedNumCaveats      int
		expectedDefinitionNames []string
		expectedCaveatNames     []string
	}

	tcs := []testcase{
		{
			name: "basic schema with single definition",
			schemaText: `
			definition user {}
		`,
			expectedNumDefinitions:  1,
			expectedNumCaveats:      0,
			expectedDefinitionNames: []string{"user"},
			expectedCaveatNames:     []string{},
		},
		{
			name: "schema with relations and permissions",
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
			expectedNumDefinitions:  3,
			expectedNumCaveats:      0,
			expectedDefinitionNames: []string{"user", "organization", "resource"},
			expectedCaveatNames:     []string{},
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
			expectedNumDefinitions:  2,
			expectedNumCaveats:      1,
			expectedDefinitionNames: []string{"user", "resource"},
			expectedCaveatNames:     []string{"must_be_ip_address"},
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
			expectedNumDefinitions:  2,
			expectedNumCaveats:      2,
			expectedDefinitionNames: []string{"user", "resource"},
			expectedCaveatNames:     []string{"ip_check", "time_check"},
		},
		{
			name: "complex schema with operations",
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
				permission admin_view = org->admin & viewer
			}
		`,
			expectedNumDefinitions:  3,
			expectedNumCaveats:      0,
			expectedDefinitionNames: []string{"user", "organization", "resource"},
			expectedCaveatNames:     []string{},
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

			// Convert back to CompiledSchema using AsCompiledSchema
			reconverted, err := v2Schema.AsCompiledSchema()
			require.NoError(t, err)
			require.NotNil(t, reconverted)

			// Verify the reconverted schema has the expected number of definitions
			require.Len(t, reconverted.ObjectDefinitions, tc.expectedNumDefinitions)
			require.Len(t, reconverted.CaveatDefinitions, tc.expectedNumCaveats)
			require.Len(t, reconverted.OrderedDefinitions, tc.expectedNumDefinitions+tc.expectedNumCaveats)

			// Verify all expected definition names are present
			definitionNames := make(map[string]bool)
			for _, def := range reconverted.ObjectDefinitions {
				definitionNames[def.Name] = true
			}
			for _, expectedName := range tc.expectedDefinitionNames {
				require.True(t, definitionNames[expectedName], "Expected definition %s to be present", expectedName)
			}

			// Verify all expected caveat names are present
			caveatNames := make(map[string]bool)
			for _, caveat := range reconverted.CaveatDefinitions {
				caveatNames[caveat.Name] = true
			}
			for _, expectedName := range tc.expectedCaveatNames {
				require.True(t, caveatNames[expectedName], "Expected caveat %s to be present", expectedName)
			}
		})
	}
}

func TestAsCompiledSchemaRoundTrip(t *testing.T) {
	t.Parallel()

	schemaText := `
		caveat ip_check(ip string) {
			ip == "192.168.1.1"
		}

		definition user {}

		definition organization {
			relation member: user
			relation admin: user
		}

		definition resource {
			relation org: organization
			relation viewer: user with ip_check
			relation banned: user
			permission view = (org->member + viewer) - banned
			permission admin = org->admin
		}
	`

	// Compile the original schema
	compiled1, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to v2 schema
	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled1)
	require.NoError(t, err)

	// Convert back to CompiledSchema
	compiled2, err := v2Schema.AsCompiledSchema()
	require.NoError(t, err)

	// Verify that the two compiled schemas have the same structure
	require.Len(t, compiled2.ObjectDefinitions, len(compiled1.ObjectDefinitions))
	require.Len(t, compiled2.CaveatDefinitions, len(compiled1.CaveatDefinitions))

	// Verify definition names match
	for i, def1 := range compiled1.ObjectDefinitions {
		def2Found := false
		for _, def2 := range compiled2.ObjectDefinitions {
			if def2.Name == def1.Name {
				def2Found = true
				require.Len(t, def2.Relation, len(def1.Relation), "Definition %s should have same number of relations", def1.Name)
				break
			}
		}
		require.True(t, def2Found, "Definition %s should exist in reconverted schema", compiled1.ObjectDefinitions[i].Name)
	}

	// Verify caveat names match
	for i, caveat1 := range compiled1.CaveatDefinitions {
		caveat2Found := false
		for _, caveat2 := range compiled2.CaveatDefinitions {
			if caveat2.Name == caveat1.Name {
				caveat2Found = true
				require.Equal(t, caveat1.SerializedExpression, caveat2.SerializedExpression, "Caveat %s should have same expression", caveat1.Name)
				break
			}
		}
		require.True(t, caveat2Found, "Caveat %s should exist in reconverted schema", compiled1.CaveatDefinitions[i].Name)
	}

	// Verify that we can build a v2 schema from the reconverted compiled schema
	v2SchemaFromReconverted, err := BuildSchemaFromCompiledSchema(*compiled2)
	require.NoError(t, err)
	require.NotNil(t, v2SchemaFromReconverted)

	// Verify both v2 schemas have the same number of definitions and caveats
	require.Len(t, v2SchemaFromReconverted.Definitions(), len(v2Schema.Definitions()))
	require.Len(t, v2SchemaFromReconverted.Caveats(), len(v2Schema.Caveats()))
}

func TestAsCompiledSchemaNilSchema(t *testing.T) {
	t.Parallel()

	var nilSchema *Schema
	compiled, err := nilSchema.AsCompiledSchema()
	require.Error(t, err)
	require.Nil(t, compiled)
	require.Contains(t, err.Error(), "cannot convert nil schema")
}

func TestAsCompiledSchemaEmptySchema(t *testing.T) {
	t.Parallel()

	schemaText := ``

	// Compile an empty schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to v2 schema
	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Convert back to CompiledSchema
	reconverted, err := v2Schema.AsCompiledSchema()
	require.NoError(t, err)
	require.NotNil(t, reconverted)

	// Verify empty schema
	require.Empty(t, reconverted.ObjectDefinitions)
	require.Empty(t, reconverted.CaveatDefinitions)
	require.Empty(t, reconverted.OrderedDefinitions)
}

func TestAsCompiledSchemaOrderedDefinitions(t *testing.T) {
	t.Parallel()

	schemaText := `
		caveat caveat1(x string) { x == "a" }

		definition def1 {}

		caveat caveat2(y string) { y == "b" }

		definition def2 {}
	`

	// Compile the schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to v2 schema
	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Convert back to CompiledSchema
	reconverted, err := v2Schema.AsCompiledSchema()
	require.NoError(t, err)

	// Verify OrderedDefinitions contains all definitions
	require.Len(t, reconverted.OrderedDefinitions, 4)

	// Verify all definitions are present in OrderedDefinitions
	names := make(map[string]bool)
	for _, def := range reconverted.OrderedDefinitions {
		names[def.GetName()] = true
	}
	require.True(t, names["def1"])
	require.True(t, names["def2"])
	require.True(t, names["caveat1"])
	require.True(t, names["caveat2"])
}
