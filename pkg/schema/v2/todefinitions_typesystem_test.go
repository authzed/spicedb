package schema

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	pkgschema "github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// definitionTester is a function that tests a definition using the type system
type definitionTester func(t *testing.T, def *pkgschema.Definition)

func TestToDefinitionsRoundTripWithTypeSystem(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name        string
		schemaText  string
		definitions map[string]definitionTester
	}

	tcs := []testcase{
		{
			name: "basic relations and permissions",
			schemaText: `
				definition user {}

				definition resource {
					relation viewer: user
					relation editor: user
					permission view = viewer
					permission edit = editor
					permission admin = viewer + editor
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer relation", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))
						require.True(t, def.HasTypeInformation("viewer"))

						allowed, err := def.IsAllowedDirectNamespace("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, allowed)

						allowedRels, err := def.GetAllowedDirectNamespaceSubjectRelations("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, []string{"..."}, allowedRels.AsSlice())

						wildcardAllowed, err := def.IsAllowedPublicNamespace("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.PublicSubjectNotAllowed, wildcardAllowed)
					})

					t.Run("editor relation", func(t *testing.T) {
						require.True(t, def.HasRelation("editor"))
						require.False(t, def.IsPermission("editor"))
						require.True(t, def.HasTypeInformation("editor"))
					})

					t.Run("view permission", func(t *testing.T) {
						require.True(t, def.HasRelation("view"))
						require.True(t, def.IsPermission("view"))
						require.False(t, def.HasTypeInformation("view"))
					})

					t.Run("edit permission", func(t *testing.T) {
						require.True(t, def.HasRelation("edit"))
						require.True(t, def.IsPermission("edit"))
						require.False(t, def.HasTypeInformation("edit"))
					})

					t.Run("admin permission", func(t *testing.T) {
						require.True(t, def.HasRelation("admin"))
						require.True(t, def.IsPermission("admin"))
						require.False(t, def.HasTypeInformation("admin"))
					})
				},
			},
		},
		{
			name: "relations with wildcards",
			schemaText: `
				definition user {}

				definition resource {
					relation viewer: user:*
					relation editor: user
					permission view = viewer
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer with wildcard", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))
						require.True(t, def.HasTypeInformation("viewer"))

						// Wildcard means only user:* is allowed, not regular user subjects
						wildcardAllowed, err := def.IsAllowedPublicNamespace("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.PublicSubjectAllowed, wildcardAllowed)

						// Non-wildcard subjects should return empty
						allowedRels, err := def.GetAllowedDirectNamespaceSubjectRelations("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, 0, allowedRels.Len())
					})

					t.Run("editor without wildcard", func(t *testing.T) {
						require.True(t, def.HasRelation("editor"))
						require.False(t, def.IsPermission("editor"))

						wildcardAllowed, err := def.IsAllowedPublicNamespace("editor", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.PublicSubjectNotAllowed, wildcardAllowed)

						allowed, err := def.IsAllowedDirectNamespace("editor", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, allowed)
					})
				},
			},
		},
		{
			name: "relations with specific subject relations",
			schemaText: `
				definition user {}

				definition group {
					relation member: user
				}

				definition resource {
					relation viewer: user | group#member
					relation owner: user
					permission view = viewer
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer with multiple types", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))

						// Check user is allowed
						userAllowed, err := def.IsAllowedDirectNamespace("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, userAllowed)

						// Check group is allowed
						groupAllowed, err := def.IsAllowedDirectNamespace("viewer", "group")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, groupAllowed)

						// Check specific relations
						userRel, err := def.IsAllowedDirectRelation("viewer", "user", "...")
						require.NoError(t, err)
						require.Equal(t, pkgschema.DirectRelationValid, userRel)

						groupMemberRel, err := def.IsAllowedDirectRelation("viewer", "group", "member")
						require.NoError(t, err)
						require.Equal(t, pkgschema.DirectRelationValid, groupMemberRel)

						// group#other should not be allowed
						groupOtherRel, err := def.IsAllowedDirectRelation("viewer", "group", "other")
						require.NoError(t, err)
						require.Equal(t, pkgschema.DirectRelationNotValid, groupOtherRel)
					})

					t.Run("owner single type", func(t *testing.T) {
						require.True(t, def.HasRelation("owner"))
						require.False(t, def.IsPermission("owner"))

						userAllowed, err := def.IsAllowedDirectNamespace("owner", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, userAllowed)

						// group should not be allowed
						groupAllowed, err := def.IsAllowedDirectNamespace("owner", "group")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionNotValid, groupAllowed)
					})
				},
			},
		},
		{
			name: "complex permissions with arrows",
			schemaText: `
				definition user {}

				definition organization {
					relation member: user
					relation admin: user
				}

				definition resource {
					relation org: organization
					relation viewer: user
					permission view = org->member + viewer
					permission admin = org->admin
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("org relation", func(t *testing.T) {
						require.True(t, def.HasRelation("org"))
						require.False(t, def.IsPermission("org"))

						orgAllowed, err := def.IsAllowedDirectNamespace("org", "organization")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, orgAllowed)

						allowedRels, err := def.GetAllowedDirectNamespaceSubjectRelations("org", "organization")
						require.NoError(t, err)
						require.Equal(t, []string{"..."}, allowedRels.AsSlice())
					})

					t.Run("view permission with arrow", func(t *testing.T) {
						require.True(t, def.HasRelation("view"))
						require.True(t, def.IsPermission("view"))
						require.False(t, def.HasTypeInformation("view"))
					})

					t.Run("admin permission with arrow", func(t *testing.T) {
						require.True(t, def.HasRelation("admin"))
						require.True(t, def.IsPermission("admin"))
						require.False(t, def.HasTypeInformation("admin"))
					})
				},
			},
		},
		{
			name: "multi-type relations with ellipsis",
			schemaText: `
				definition user {}
				definition team {}

				definition organization {
					relation admin: user | team
				}
			`,
			definitions: map[string]definitionTester{
				"organization": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("admin multi-type", func(t *testing.T) {
						require.True(t, def.HasRelation("admin"))
						require.False(t, def.IsPermission("admin"))

						userAllowed, err := def.IsAllowedDirectNamespace("admin", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, userAllowed)

						teamAllowed, err := def.IsAllowedDirectNamespace("admin", "team")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, teamAllowed)

						userRels, err := def.GetAllowedDirectNamespaceSubjectRelations("admin", "user")
						require.NoError(t, err)
						require.Contains(t, userRels.AsSlice(), "...")

						teamRels, err := def.GetAllowedDirectNamespaceSubjectRelations("admin", "team")
						require.NoError(t, err)
						require.Contains(t, teamRels.AsSlice(), "...")
					})
				},
			},
		},
		{
			name: "relations with caveats",
			schemaText: `
				caveat ip_check(ip string) {
					ip == "127.0.0.1"
				}

				definition user {}

				definition resource {
					relation viewer: user with ip_check
					relation public_viewer: user
					permission view = viewer + public_viewer
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer with caveat", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))

						userAllowed, err := def.IsAllowedDirectNamespace("viewer", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, userAllowed)
					})

					t.Run("public_viewer without caveat", func(t *testing.T) {
						require.True(t, def.HasRelation("public_viewer"))
						require.False(t, def.IsPermission("public_viewer"))

						userAllowed, err := def.IsAllowedDirectNamespace("public_viewer", "user")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, userAllowed)
					})

					t.Run("view permission", func(t *testing.T) {
						require.True(t, def.IsPermission("view"))
					})
				},
			},
		},
		{
			name: "permissions with intersections and exclusions",
			schemaText: `
				definition user {}

				definition resource {
					relation viewer: user
					relation editor: user
					relation banned: user
					permission view = (viewer + editor) - banned
					permission admin_view = viewer & editor
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer relation", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))
						require.True(t, def.HasTypeInformation("viewer"))
					})

					t.Run("editor relation", func(t *testing.T) {
						require.True(t, def.HasRelation("editor"))
						require.False(t, def.IsPermission("editor"))
						require.True(t, def.HasTypeInformation("editor"))
					})

					t.Run("banned relation", func(t *testing.T) {
						require.True(t, def.HasRelation("banned"))
						require.False(t, def.IsPermission("banned"))
						require.True(t, def.HasTypeInformation("banned"))
					})

					t.Run("view permission with exclusion", func(t *testing.T) {
						require.True(t, def.HasRelation("view"))
						require.True(t, def.IsPermission("view"))
						require.False(t, def.HasTypeInformation("view"))
					})

					t.Run("admin_view permission with intersection", func(t *testing.T) {
						require.True(t, def.HasRelation("admin_view"))
						require.True(t, def.IsPermission("admin_view"))
						require.False(t, def.HasTypeInformation("admin_view"))
					})
				},
			},
		},
	}

	ctx := context.Background()

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			// Step 1: Compile original schema
			compiled1, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Step 2: Convert to v2 schema
			v2Schema, err := BuildSchemaFromCompiledSchema(*compiled1)
			require.NoError(t, err)

			// Step 3: Convert back to v1
			defs, caveats, err := v2Schema.ToDefinitions()
			require.NoError(t, err)

			// Step 4: Create a type system from the converted definitions
			compiled2 := &compiler.CompiledSchema{
				ObjectDefinitions: defs,
				CaveatDefinitions: caveats,
			}
			resolver := pkgschema.ResolverForSchema(compiled2)
			ts := pkgschema.NewTypeSystem(resolver)

			// Step 5: Run tester functions for each definition
			for defName, tester := range tc.definitions {
				t.Run(defName, func(t *testing.T) {
					def, err := ts.GetDefinition(ctx, defName)
					require.NoError(t, err)
					require.NotNil(t, def)

					// Run the tester function
					tester(t, def)
				})
			}
		})
	}
}

func TestToDefinitionsFlattenedRoundTripWithTypeSystem(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name        string
		schemaText  string
		definitions map[string]definitionTester
	}

	tcs := []testcase{
		{
			name: "basic nested unions",
			schemaText: `
				definition user {}

				definition resource {
					relation viewer: user
					relation editor: user
					permission view = viewer + (editor + viewer)
					permission edit = editor
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer relation", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))
						require.True(t, def.HasTypeInformation("viewer"))
					})

					t.Run("editor relation", func(t *testing.T) {
						require.True(t, def.HasRelation("editor"))
						require.False(t, def.IsPermission("editor"))
						require.True(t, def.HasTypeInformation("editor"))
					})

					t.Run("view permission", func(t *testing.T) {
						require.True(t, def.HasRelation("view"))
						require.True(t, def.IsPermission("view"))
						require.False(t, def.HasTypeInformation("view"))
					})

					t.Run("edit permission", func(t *testing.T) {
						require.True(t, def.HasRelation("edit"))
						require.True(t, def.IsPermission("edit"))
						require.False(t, def.HasTypeInformation("edit"))
					})

					t.Run("synthetic permissions if present are valid", func(t *testing.T) {
						// Note: Simple nested unions may not create synthetic permissions
						// after BDD canonicalization, so we only validate if they exist
						syntheticPerms := findSyntheticPermissions(def)

						// Validate each synthetic permission if any exist
						for _, permName := range syntheticPerms {
							t.Run(permName, func(t *testing.T) {
								require.True(t, def.HasRelation(permName), "Synthetic permission should exist")
								require.True(t, def.IsPermission(permName), "Synthetic relation should be a permission")
								require.False(t, def.HasTypeInformation(permName), "Synthetic permission should not have type information")
							})
						}
					})
				},
			},
		},
		{
			name: "complex nested operations",
			schemaText: `
				definition user {}

				definition organization {
					relation member: user
					relation admin: user
				}

				definition resource {
					relation org: organization
					relation viewer: user
					relation editor: user
					permission view = org->member + (viewer + editor)
					permission admin = org->admin + editor
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("org relation", func(t *testing.T) {
						require.True(t, def.HasRelation("org"))
						require.False(t, def.IsPermission("org"))

						orgAllowed, err := def.IsAllowedDirectNamespace("org", "organization")
						require.NoError(t, err)
						require.Equal(t, pkgschema.AllowedDefinitionValid, orgAllowed)
					})

					t.Run("viewer relation", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))
						require.True(t, def.HasTypeInformation("viewer"))
					})

					t.Run("view permission", func(t *testing.T) {
						require.True(t, def.HasRelation("view"))
						require.True(t, def.IsPermission("view"))
						require.False(t, def.HasTypeInformation("view"))
					})

					t.Run("admin permission", func(t *testing.T) {
						require.True(t, def.HasRelation("admin"))
						require.True(t, def.IsPermission("admin"))
						require.False(t, def.HasTypeInformation("admin"))
					})

					t.Run("synthetic permissions exist and are valid", func(t *testing.T) {
						// Find all synthetic permissions (those containing __)
						syntheticPerms := findSyntheticPermissions(def)
						require.NotEmpty(t, syntheticPerms, "Expected synthetic permissions to be created for nested operations")

						// Validate each synthetic permission
						for _, permName := range syntheticPerms {
							t.Run(permName, func(t *testing.T) {
								require.True(t, def.HasRelation(permName), "Synthetic permission should exist")
								require.True(t, def.IsPermission(permName), "Synthetic relation should be a permission")
								require.False(t, def.HasTypeInformation(permName), "Synthetic permission should not have type information")
							})
						}
					})
				},
			},
		},
		{
			name: "deeply nested with intersections",
			schemaText: `
				definition user {}

				definition resource {
					relation viewer: user
					relation editor: user
					relation owner: user
					permission view = viewer + (editor & owner)
					permission full = (viewer + editor) & owner
				}
			`,
			definitions: map[string]definitionTester{
				"resource": func(t *testing.T, def *pkgschema.Definition) {
					t.Run("viewer relation", func(t *testing.T) {
						require.True(t, def.HasRelation("viewer"))
						require.False(t, def.IsPermission("viewer"))
						require.True(t, def.HasTypeInformation("viewer"))
					})

					t.Run("editor relation", func(t *testing.T) {
						require.True(t, def.HasRelation("editor"))
						require.False(t, def.IsPermission("editor"))
						require.True(t, def.HasTypeInformation("editor"))
					})

					t.Run("owner relation", func(t *testing.T) {
						require.True(t, def.HasRelation("owner"))
						require.False(t, def.IsPermission("owner"))
						require.True(t, def.HasTypeInformation("owner"))
					})

					t.Run("view permission", func(t *testing.T) {
						require.True(t, def.HasRelation("view"))
						require.True(t, def.IsPermission("view"))
						require.False(t, def.HasTypeInformation("view"))
					})

					t.Run("full permission", func(t *testing.T) {
						require.True(t, def.HasRelation("full"))
						require.True(t, def.IsPermission("full"))
						require.False(t, def.HasTypeInformation("full"))
					})

					t.Run("synthetic permissions exist and are valid", func(t *testing.T) {
						// Find all synthetic permissions (those containing __)
						syntheticPerms := findSyntheticPermissions(def)
						require.NotEmpty(t, syntheticPerms, "Expected synthetic permissions to be created for nested operations")

						// Validate each synthetic permission
						for _, permName := range syntheticPerms {
							t.Run(permName, func(t *testing.T) {
								require.True(t, def.HasRelation(permName), "Synthetic permission should exist")
								require.True(t, def.IsPermission(permName), "Synthetic relation should be a permission")
								require.False(t, def.HasTypeInformation(permName), "Synthetic permission should not have type information")
							})
						}
					})
				},
			},
		},
	}

	ctx := context.Background()

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			// Step 1: Compile original schema
			compiled1, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Step 2: Convert to v2 schema
			v2Schema, err := BuildSchemaFromCompiledSchema(*compiled1)
			require.NoError(t, err)

			// Step 3: Resolve the schema
			resolved, err := ResolveSchema(v2Schema)
			require.NoError(t, err)

			// Step 4: Flatten the schema
			flattened, err := FlattenSchema(resolved, FlattenSeparatorDoubleUnderscore)
			require.NoError(t, err)

			// Step 5: Convert flattened schema back to v1
			defs, caveats, err := flattened.ResolvedSchema().Schema().ToDefinitions()
			require.NoError(t, err)

			// Step 6: Create a type system from the converted flattened definitions
			compiled2 := &compiler.CompiledSchema{
				ObjectDefinitions: defs,
				CaveatDefinitions: caveats,
			}
			resolver := pkgschema.ResolverForSchema(compiled2)
			ts := pkgschema.NewTypeSystem(resolver)

			// Step 7: Run tester functions for each definition
			for defName, tester := range tc.definitions {
				t.Run(defName, func(t *testing.T) {
					def, err := ts.GetDefinition(ctx, defName)
					require.NoError(t, err)
					require.NotNil(t, def)

					// Run the tester function
					tester(t, def)
				})
			}
		})
	}
}

func TestToDefinitionsRoundTripWithTypeSystemNegativeCases(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}
		definition team {}

		definition organization {
			relation member: user
			relation admin: user
		}

		definition resource {
			relation viewer: user
			relation org: organization
			permission view = org->member
		}
	`

	ctx := context.Background()

	// Compile and convert
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	defs, caveats, err := v2Schema.ToDefinitions()
	require.NoError(t, err)

	// Create type system
	compiled2 := &compiler.CompiledSchema{
		ObjectDefinitions: defs,
		CaveatDefinitions: caveats,
	}
	resolver := pkgschema.ResolverForSchema(compiled2)
	ts := pkgschema.NewTypeSystem(resolver)

	def, err := ts.GetDefinition(ctx, "resource")
	require.NoError(t, err)

	t.Run("disallowed namespace", func(t *testing.T) {
		t.Parallel()
		// team is not allowed on viewer relation
		allowed, err := def.IsAllowedDirectNamespace("viewer", "team")
		require.NoError(t, err)
		require.Equal(t, pkgschema.AllowedDefinitionNotValid, allowed)
	})

	t.Run("disallowed relation on allowed namespace", func(t *testing.T) {
		t.Parallel()
		// organization is allowed on org relation, but not with the "admin" relation
		allowed, err := def.IsAllowedDirectRelation("org", "organization", "admin")
		require.NoError(t, err)
		require.Equal(t, pkgschema.DirectRelationNotValid, allowed)
	})

	t.Run("non-existent relation", func(t *testing.T) {
		t.Parallel()
		// nonexistent relation should return false
		exists := def.HasRelation("nonexistent")
		require.False(t, exists)

		isPermission := def.IsPermission("nonexistent")
		require.False(t, isPermission)

		hasTypeInfo := def.HasTypeInformation("nonexistent")
		require.False(t, hasTypeInfo)
	})

	t.Run("permission has no type information", func(t *testing.T) {
		t.Parallel()
		// view is a permission, so it should not have type information
		hasTypeInfo := def.HasTypeInformation("view")
		require.False(t, hasTypeInfo)
	})
}

// findSyntheticPermissions returns all relation names that contain the synthetic permission
// separator (double underscore), indicating they were created during schema flattening.
func findSyntheticPermissions(def *pkgschema.Definition) []string {
	var synthetic []string
	nsDef := def.Namespace()

	for _, rel := range nsDef.Relation {
		if containsDoubleUnderscore(rel.Name) {
			synthetic = append(synthetic, rel.Name)
		}
	}

	return synthetic
}

// containsDoubleUnderscore checks if a string contains "__"
func containsDoubleUnderscore(s string) bool {
	for i := 0; i < len(s)-1; i++ {
		if s[i] == '_' && s[i+1] == '_' {
			return true
		}
	}
	return false
}
