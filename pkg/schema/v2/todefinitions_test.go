package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestToDefinitionsBasic(t *testing.T) {
	t.Parallel()

	type testcase struct {
		name               string
		schemaText         string
		expectedNumDefs    int
		expectedNumCaveats int
		checkDefinition    func(t *testing.T, defs []*corev1.NamespaceDefinition, caveats []*corev1.CaveatDefinition)
	}

	tcs := []testcase{
		{
			name: "simple schema with single definition",
			schemaText: `
				definition user {}
			`,
			expectedNumDefs:    1,
			expectedNumCaveats: 0,
			checkDefinition: func(t *testing.T, defs []*corev1.NamespaceDefinition, caveats []*corev1.CaveatDefinition) {
				require.Equal(t, "user", defs[0].Name)
				require.Empty(t, defs[0].Relation)
			},
		},
		{
			name: "schema with relation",
			schemaText: `
				definition user {}
				definition resource {
					relation viewer: user
				}
			`,
			expectedNumDefs:    2,
			expectedNumCaveats: 0,
			checkDefinition: func(t *testing.T, defs []*corev1.NamespaceDefinition, caveats []*corev1.CaveatDefinition) {
				var resourceDef *corev1.NamespaceDefinition
				for _, def := range defs {
					if def.Name == "resource" {
						resourceDef = def
						break
					}
				}
				require.NotNil(t, resourceDef)
				require.Len(t, resourceDef.Relation, 1)
				require.Equal(t, "viewer", resourceDef.Relation[0].Name)
				require.NotNil(t, resourceDef.Relation[0].TypeInformation)
			},
		},
		{
			name: "schema with permission",
			schemaText: `
				definition user {}
				definition resource {
					relation viewer: user
					permission view = viewer
				}
			`,
			expectedNumDefs:    2,
			expectedNumCaveats: 0,
			checkDefinition: func(t *testing.T, defs []*corev1.NamespaceDefinition, caveats []*corev1.CaveatDefinition) {
				var resourceDef *corev1.NamespaceDefinition
				for _, def := range defs {
					if def.Name == "resource" {
						resourceDef = def
						break
					}
				}
				require.NotNil(t, resourceDef)
				require.Len(t, resourceDef.Relation, 2) // relation + permission

				// Find the permission
				var permRelation *corev1.Relation
				for _, rel := range resourceDef.Relation {
					if rel.Name == "view" {
						permRelation = rel
						break
					}
				}
				require.NotNil(t, permRelation)
				require.NotNil(t, permRelation.UsersetRewrite)
				require.Nil(t, permRelation.TypeInformation)
			},
		},
		{
			name: "schema with caveat",
			schemaText: `
				caveat ip_check(ip string) {
					ip == "127.0.0.1"
				}

				definition user {}
				definition resource {
					relation viewer: user with ip_check
				}
			`,
			expectedNumDefs:    2,
			expectedNumCaveats: 1,
			checkDefinition: func(t *testing.T, defs []*corev1.NamespaceDefinition, caveats []*corev1.CaveatDefinition) {
				require.Len(t, caveats, 1)
				require.Equal(t, "ip_check", caveats[0].Name)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tc := tc
			t.Parallel()

			// Compile schema
			compiled, err := compiler.Compile(compiler.InputSchema{
				Source:       input.Source("test"),
				SchemaString: tc.schemaText,
			}, compiler.AllowUnprefixedObjectType())
			require.NoError(t, err)

			// Build v2 schema
			v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
			require.NoError(t, err)

			// Convert to definitions
			defs, caveats, err := v2Schema.ToDefinitions()
			require.NoError(t, err)
			require.Len(t, defs, tc.expectedNumDefs)
			require.Len(t, caveats, tc.expectedNumCaveats)

			// Run custom checks
			if tc.checkDefinition != nil {
				tc.checkDefinition(t, defs, caveats)
			}
		})
	}
}

func TestToDefinitionsMetadataPreservation(t *testing.T) {
	t.Parallel()

	t.Run("definition metadata is preserved", func(t *testing.T) {
		t.Parallel()

		// Create a definition with metadata
		docComment := &implv1.DocComment{
			Comment: "This is a test definition",
		}
		docCommentAny, err := anypb.New(docComment)
		require.NoError(t, err)

		originalDef := &corev1.NamespaceDefinition{
			Name: "test_resource",
			Metadata: &corev1.Metadata{
				MetadataMessage: []*anypb.Any{docCommentAny},
			},
		}

		// Convert to v2
		v2Schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{originalDef}, nil)
		require.NoError(t, err)

		// Convert back to v1
		defs, _, err := v2Schema.ToDefinitions()
		require.NoError(t, err)
		require.Len(t, defs, 1)

		// Check metadata is preserved
		require.NotNil(t, defs[0].Metadata)
		require.Len(t, defs[0].Metadata.MetadataMessage, 1)

		var comment implv1.DocComment
		err = defs[0].Metadata.MetadataMessage[0].UnmarshalTo(&comment)
		require.NoError(t, err)
		require.Equal(t, "This is a test definition", comment.Comment)
	})

	t.Run("relation metadata with comments is preserved", func(t *testing.T) {
		t.Parallel()

		// Create relation with doc comment
		docComment := &implv1.DocComment{
			Comment: "This is a viewer relation",
		}
		docCommentAny, err := anypb.New(docComment)
		require.NoError(t, err)

		relationMetadata := &implv1.RelationMetadata{
			Kind: implv1.RelationMetadata_RELATION,
		}
		relationMetadataAny, err := anypb.New(relationMetadata)
		require.NoError(t, err)

		originalDef := &corev1.NamespaceDefinition{
			Name: "test_resource",
			Relation: []*corev1.Relation{
				{
					Name: "viewer",
					TypeInformation: &corev1.TypeInformation{
						AllowedDirectRelations: []*corev1.AllowedRelation{
							{
								Namespace: "user",
								RelationOrWildcard: &corev1.AllowedRelation_Relation{
									Relation: "",
								},
							},
						},
					},
					Metadata: &corev1.Metadata{
						MetadataMessage: []*anypb.Any{docCommentAny, relationMetadataAny},
					},
				},
			},
		}

		// Convert to v2
		v2Schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{originalDef}, nil)
		require.NoError(t, err)

		// Convert back to v1
		defs, _, err := v2Schema.ToDefinitions()
		require.NoError(t, err)
		require.Len(t, defs, 1)
		require.Len(t, defs[0].Relation, 1)

		// Check metadata is preserved
		require.NotNil(t, defs[0].Relation[0].Metadata)
		require.Len(t, defs[0].Relation[0].Metadata.MetadataMessage, 2)

		// Verify both metadata types are present
		var foundComment, foundRelationKind bool
		for _, msg := range defs[0].Relation[0].Metadata.MetadataMessage {
			var dc implv1.DocComment
			if err := msg.UnmarshalTo(&dc); err == nil {
				require.Equal(t, "This is a viewer relation", dc.Comment)
				foundComment = true
				continue
			}

			var rm implv1.RelationMetadata
			if err := msg.UnmarshalTo(&rm); err == nil {
				require.Equal(t, implv1.RelationMetadata_RELATION, rm.Kind)
				foundRelationKind = true
			}
		}

		require.True(t, foundComment)
		require.True(t, foundRelationKind)
	})

	t.Run("caveat metadata is preserved", func(t *testing.T) {
		t.Parallel()

		// Create caveat with metadata
		docComment := &implv1.DocComment{
			Comment: "This checks IP addresses",
		}
		docCommentAny, err := anypb.New(docComment)
		require.NoError(t, err)

		originalCaveat := &corev1.CaveatDefinition{
			Name:                 "ip_check",
			SerializedExpression: []byte(`ip == "127.0.0.1"`),
			Metadata: &corev1.Metadata{
				MetadataMessage: []*anypb.Any{docCommentAny},
			},
		}

		// Convert to v2
		v2Schema, err := BuildSchemaFromDefinitions(nil, []*corev1.CaveatDefinition{originalCaveat})
		require.NoError(t, err)

		// Convert back to v1
		_, caveats, err := v2Schema.ToDefinitions()
		require.NoError(t, err)
		require.Len(t, caveats, 1)

		// Check metadata is preserved
		require.NotNil(t, caveats[0].Metadata)

		var comment implv1.DocComment
		err = caveats[0].Metadata.MetadataMessage[0].UnmarshalTo(&comment)
		require.NoError(t, err)
		require.Equal(t, "This checks IP addresses", comment.Comment)
	})
}

func TestToDefinitionsRelationsHaveProperMetadata(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user
			relation editor: user
		}
	`

	// Compile and build v2 schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Convert to v1
	defs, _, err := v2Schema.ToDefinitions()
	require.NoError(t, err)

	// Find the resource definition
	var resourceDef *corev1.NamespaceDefinition
	for _, def := range defs {
		if def.Name == "resource" {
			resourceDef = def
			break
		}
	}
	require.NotNil(t, resourceDef)

	// Check that all relations have proper TypeInformation
	for _, rel := range resourceDef.Relation {
		require.NotNil(t, rel.TypeInformation, "Relation %s should have TypeInformation", rel.Name)
		require.Nil(t, rel.UsersetRewrite, "Relation %s should not have UsersetRewrite", rel.Name)
	}
}

func TestToDefinitionsPermissionsHaveProperMetadata(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user
			relation editor: user
			permission view = viewer
			permission edit = editor
			permission admin = viewer + editor
		}
	`

	// Compile and build v2 schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Convert to v1
	defs, _, err := v2Schema.ToDefinitions()
	require.NoError(t, err)

	// Find the resource definition
	var resourceDef *corev1.NamespaceDefinition
	for _, def := range defs {
		if def.Name == "resource" {
			resourceDef = def
			break
		}
	}
	require.NotNil(t, resourceDef)

	// Separate relations and permissions
	var relations, permissions []*corev1.Relation
	for _, rel := range resourceDef.Relation {
		if rel.UsersetRewrite != nil {
			permissions = append(permissions, rel)
		} else {
			relations = append(relations, rel)
		}
	}

	require.Len(t, relations, 2, "Should have 2 relations")
	require.Len(t, permissions, 3, "Should have 3 permissions")

	// Check that permissions have UsersetRewrite and no TypeInformation
	for _, perm := range permissions {
		require.NotNil(t, perm.UsersetRewrite, "Permission %s should have UsersetRewrite", perm.Name)
		require.Nil(t, perm.TypeInformation, "Permission %s should not have TypeInformation", perm.Name)

		// Check that permissions have metadata marking them as permissions
		require.NotNil(t, perm.Metadata, "Permission %s should have metadata", perm.Name)

		// Parse and verify the metadata
		var foundPermissionMetadata bool
		for _, msg := range perm.Metadata.MetadataMessage {
			var rm implv1.RelationMetadata
			if err := msg.UnmarshalTo(&rm); err == nil {
				require.Equal(t, implv1.RelationMetadata_PERMISSION, rm.Kind, "Permission %s should be marked as PERMISSION", perm.Name)
				foundPermissionMetadata = true
			}
		}
		require.True(t, foundPermissionMetadata, "Permission %s should have RelationMetadata marking it as a permission", perm.Name)
	}

	// Check that relations have metadata marking them as relations
	for _, rel := range relations {
		require.NotNil(t, rel.Metadata, "Relation %s should have metadata", rel.Name)

		// Parse and verify the metadata
		var foundRelationMetadata bool
		for _, msg := range rel.Metadata.MetadataMessage {
			var rm implv1.RelationMetadata
			if err := msg.UnmarshalTo(&rm); err == nil {
				require.Equal(t, implv1.RelationMetadata_RELATION, rm.Kind, "Relation %s should be marked as RELATION", rel.Name)
				foundRelationMetadata = true
			}
		}
		require.True(t, foundRelationMetadata, "Relation %s should have RelationMetadata marking it as a relation", rel.Name)
	}
}

func TestToDefinitionsFlattenedSchema(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition resource {
			relation viewer: user
			relation editor: user
			permission view = viewer + (editor + viewer)
		}
	`

	// Compile and build v2 schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve and flatten the schema
	resolved, err := ResolveSchema(v2Schema)
	require.NoError(t, err)

	flattened, err := FlattenSchema(resolved, FlattenSeparatorDoubleUnderscore)
	require.NoError(t, err)

	// Convert flattened schema to v1
	defs, _, err := flattened.ResolvedSchema().Schema().ToDefinitions()
	require.NoError(t, err)

	// Find the resource definition
	var resourceDef *corev1.NamespaceDefinition
	for _, def := range defs {
		if def.Name == "resource" {
			resourceDef = def
			break
		}
	}
	require.NotNil(t, resourceDef)

	// Find the view permission
	var viewPerm *corev1.Relation
	for _, rel := range resourceDef.Relation {
		if rel.Name == "view" {
			viewPerm = rel
			break
		}
	}
	require.NotNil(t, viewPerm)
	require.NotNil(t, viewPerm.UsersetRewrite)

	// The flattened schema should remove nested parentheses
	// Original: viewer + (editor + viewer)
	// Flattened: viewer + editor + viewer (pure unions of names are not deduplicated)
	require.NotNil(t, viewPerm.UsersetRewrite.GetUnion())
}

func TestToDefinitionsResolvedSchema(t *testing.T) {
	t.Parallel()

	schemaText := `
		definition user {}

		definition organization {
			relation member: user
		}

		definition resource {
			relation org: organization
			permission view = org->member
		}
	`

	// Compile and build v2 schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Resolve the schema
	resolved, err := ResolveSchema(v2Schema)
	require.NoError(t, err)

	// Convert resolved schema to v1
	defs, _, err := resolved.Schema().ToDefinitions()
	require.NoError(t, err)

	// Find the resource definition
	var resourceDef *corev1.NamespaceDefinition
	for _, def := range defs {
		if def.Name == "resource" {
			resourceDef = def
			break
		}
	}
	require.NotNil(t, resourceDef)

	// Find the view permission
	var viewPerm *corev1.Relation
	for _, rel := range resourceDef.Relation {
		if rel.Name == "view" {
			viewPerm = rel
			break
		}
	}
	require.NotNil(t, viewPerm)
	require.NotNil(t, viewPerm.UsersetRewrite)

	// Check that the arrow reference is preserved
	require.NotNil(t, viewPerm.UsersetRewrite.GetUnion())
	require.Len(t, viewPerm.UsersetRewrite.GetUnion().Child, 1)
	require.NotNil(t, viewPerm.UsersetRewrite.GetUnion().Child[0].GetTupleToUserset())
}

func TestToDefinitionsComplexSchema(t *testing.T) {
	t.Parallel()

	schemaText := `
		caveat ip_check(ip string) {
			ip == "127.0.0.1"
		}

		definition user {}

		definition group {
			relation member: user
		}

		definition organization {
			relation member: user | group#member
			relation admin: user
		}

		definition resource {
			relation org: organization
			relation viewer: user with ip_check
			relation banned: user
			permission view = (org->member + viewer) - banned
			permission admin_view = org->admin & view
		}
	`

	// Compile and build v2 schema
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	v2Schema, err := BuildSchemaFromCompiledSchema(*compiled)
	require.NoError(t, err)

	// Convert to v1
	defs, caveats, err := v2Schema.ToDefinitions()
	require.NoError(t, err)

	require.Len(t, defs, 4) // user, group, organization, resource
	require.Len(t, caveats, 1)

	// Find the resource definition
	var resourceDef *corev1.NamespaceDefinition
	for _, def := range defs {
		if def.Name == "resource" {
			resourceDef = def
			break
		}
	}
	require.NotNil(t, resourceDef)

	// Count relations and permissions
	var relationCount, permissionCount int
	for _, rel := range resourceDef.Relation {
		if rel.UsersetRewrite != nil {
			permissionCount++
		} else {
			relationCount++
		}
	}

	require.Equal(t, 3, relationCount, "Should have 3 relations: org, viewer, banned")
	require.Equal(t, 2, permissionCount, "Should have 2 permissions: view, admin_view")

	// Check that caveats are preserved
	require.Equal(t, "ip_check", caveats[0].Name)
}

func TestToDefinitionsRoundTrip(t *testing.T) {
	t.Parallel()

	schemaText := `
		caveat expiration(expires_at timestamp, current_time timestamp) {
			expires_at > current_time
		}

		definition user {}

		definition resource {
			relation viewer: user with expiration
			relation editor: user
			permission view = viewer
			permission edit = editor
			permission admin = view + edit
		}
	`

	// Compile original schema
	compiled1, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	// Convert to v2
	v2Schema1, err := BuildSchemaFromCompiledSchema(*compiled1)
	require.NoError(t, err)

	// Convert back to v1
	defs1, caveats1, err := v2Schema1.ToDefinitions()
	require.NoError(t, err)

	// Convert v1 to v2 again
	v2Schema2, err := BuildSchemaFromDefinitions(defs1, caveats1)
	require.NoError(t, err)

	// Convert back to v1 again
	defs2, caveats2, err := v2Schema2.ToDefinitions()
	require.NoError(t, err)

	// Verify that the two v1 schemas are equivalent
	require.Len(t, defs2, len(defs1))
	require.Len(t, caveats2, len(caveats1))

	// Check that each definition is preserved
	for i, def1 := range defs1 {
		var def2 *corev1.NamespaceDefinition
		for _, d := range defs2 {
			if d.Name == def1.Name {
				def2 = d
				break
			}
		}
		require.NotNil(t, def2, "Definition %s should be preserved in round-trip", defs1[i].Name)
		require.Equal(t, def1.Name, def2.Name)
		require.Len(t, def2.Relation, len(def1.Relation))
	}

	// Check that each caveat is preserved
	for _, caveat1 := range caveats1 {
		var caveat2 *corev1.CaveatDefinition
		for _, c := range caveats2 {
			if c.Name == caveat1.Name {
				caveat2 = c
				break
			}
		}
		require.NotNil(t, caveat2, "Caveat %s should be preserved in round-trip", caveat1.Name)
		require.Equal(t, caveat1.Name, caveat2.Name)
		require.Equal(t, caveat1.SerializedExpression, caveat2.SerializedExpression)
	}
}

func TestToDefinitionsNilSchema(t *testing.T) {
	t.Parallel()

	var nilSchema *Schema
	defs, caveats, err := nilSchema.ToDefinitions()
	require.Error(t, err)
	require.Nil(t, defs)
	require.Nil(t, caveats)
	require.Contains(t, err.Error(), "cannot convert nil schema")
}

func TestToDefinitionsEmptySchema(t *testing.T) {
	t.Parallel()

	schema := &Schema{
		definitions: make(map[string]*Definition),
		caveats:     make(map[string]*Caveat),
	}

	defs, caveats, err := schema.ToDefinitions()
	require.NoError(t, err)
	require.Empty(t, defs)
	require.Empty(t, caveats)
}
