package compiler

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func TestTypeAnnotationsIntegration(t *testing.T) {
	tests := []struct {
		name                  string
		schema                string
		prefix                ObjectPrefixOption
		definition            string
		expectedPermission    string
		expectedAnnotations   []string
		shouldContainMetadata bool
	}{
		{
			name: "single type annotation",
			schema: `use typechecking
definition user {}
definition document {
	permission view: user = user
}`,
			expectedPermission:    "view",
			expectedAnnotations:   []string{"user"},
			shouldContainMetadata: true,
		},
		{
			name: "multiple type annotations",
			schema: `use typechecking
definition user {}
definition organization {}
definition document {
	permission edit: user | organization = user
}`,
			expectedPermission:    "edit",
			expectedAnnotations:   []string{"user", "organization"},
			shouldContainMetadata: true,
		},
		{
			name: "permission without type annotation",
			schema: `use typechecking
definition user {}
definition document {
	permission read = user
}`,
			expectedPermission:    "read",
			expectedAnnotations:   nil,
			shouldContainMetadata: false,
		},
		{
			name: "mixed permissions with and without annotations",
			schema: `use typechecking
definition user {}
definition admin {}
definition document {
	permission view: user = user
	permission delete = admin
}`,
			expectedPermission:    "view", // We'll test the annotated one
			expectedAnnotations:   []string{"user"},
			shouldContainMetadata: true,
		},
		{
			name: "prefixed type annotation",
			schema: `use typechecking
definition example/user {}
definition group {
	relation member: example/user
	permission perm: example/user = member
}`,
			definition:            "group",
			expectedPermission:    "perm",
			expectedAnnotations:   []string{"example/user"},
			shouldContainMetadata: true,
		},
		{
			name: "prefixed type annotation after pipe",
			schema: `use typechecking
definition example/user {}
definition example/team {}
definition group {
	relation member: example/user | example/team
	permission perm: example/user | example/team = member
}`,
			definition:            "group",
			expectedPermission:    "perm",
			expectedAnnotations:   []string{"example/user", "example/team"},
			shouldContainMetadata: true,
		},
		{
			name: "type annotation normalized with implicit object prefix",
			schema: `use typechecking
definition user {}
definition document {
	relation viewer: user
	permission view: user = viewer
}`,
			prefix:                ObjectTypePrefix("theprefix"),
			definition:            "theprefix/document",
			expectedPermission:    "view",
			expectedAnnotations:   []string{"theprefix/user"},
			shouldContainMetadata: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix := tt.prefix
			if prefix == nil {
				prefix = AllowUnprefixedObjectType()
			}

			definition := tt.definition
			if definition == "" {
				definition = "document"
			}

			compiled, err := Compile(InputSchema{
				Source:       input.Source("test"),
				SchemaString: tt.schema,
			}, prefix)
			require.NoError(t, err)
			require.NotNil(t, compiled)

			// Find the definition and the expected permission
			var foundPermission *core.Relation
			for _, ns := range compiled.ObjectDefinitions {
				if ns.Name == definition {
					for _, rel := range ns.Relation {
						if rel.Name == tt.expectedPermission {
							foundPermission = rel
							break
						}
					}
					break
				}
			}

			require.NotNil(t, foundPermission, "Permission %s not found", tt.expectedPermission)

			if !tt.shouldContainMetadata {
				// For permissions without type annotations, PERMISSION metadata should still exist (created by namespace.Relation)
				// but type annotations should be empty
				require.NotNil(t, foundPermission.Metadata, "All permissions should have metadata")

				// Find the PERMISSION RelationMetadata
				var foundMetadata *implv1.RelationMetadata
				for _, metadataAny := range foundPermission.Metadata.MetadataMessage {
					var relationMetadata implv1.RelationMetadata
					if err := metadataAny.UnmarshalTo(&relationMetadata); err == nil {
						if relationMetadata.Kind == implv1.RelationMetadata_PERMISSION {
							foundMetadata = &relationMetadata
							break
						}
					}
				}

				require.NotNil(t, foundMetadata, "Should have PERMISSION RelationMetadata")
				if foundMetadata.TypeAnnotations != nil {
					require.Empty(t, foundMetadata.TypeAnnotations.Types, "Type annotations should be empty for permission without type annotations")
				}

				// Test the helper function for retrieving type annotations
				retrievedAnnotations := namespace.GetTypeAnnotations(foundPermission)
				require.Empty(t, retrievedAnnotations, "Retrieved type annotations should be empty")
				return
			}

			// For permissions with type annotations, verify metadata exists
			require.NotNil(t, foundPermission.Metadata, "Metadata should not be nil for permission with type annotations")
			require.NotEmpty(t, foundPermission.Metadata.MetadataMessage, "MetadataMessage should not be empty")

			// Find the RelationMetadata with PERMISSION kind
			var foundMetadata *implv1.RelationMetadata
			for _, metadataAny := range foundPermission.Metadata.MetadataMessage {
				var relationMetadata implv1.RelationMetadata
				if err := metadataAny.UnmarshalTo(&relationMetadata); err == nil {
					if relationMetadata.Kind == implv1.RelationMetadata_PERMISSION {
						foundMetadata = &relationMetadata
						break
					}
				}
			}

			require.NotNil(t, foundMetadata, "Should have PERMISSION RelationMetadata")
			require.Equal(t, implv1.RelationMetadata_PERMISSION, foundMetadata.Kind)
			require.NotNil(t, foundMetadata.TypeAnnotations, "TypeAnnotations should not be nil")
			require.Equal(t, tt.expectedAnnotations, foundMetadata.TypeAnnotations.Types)

			// Test the helper function for retrieving type annotations
			retrievedAnnotations := namespace.GetTypeAnnotations(foundPermission)
			require.Equal(t, tt.expectedAnnotations, retrievedAnnotations)
		})
	}
}

func TestTypeAnnotationPrefixRequired(t *testing.T) {
	// With a required object prefix, an unprefixed type annotation reports an error,
	// matching how unprefixed relation type references are handled.
	schema := `use typechecking
definition pfx/user {}
definition pfx/group {
	relation member: pfx/user
	permission perm: user = member
}`

	_, err := Compile(InputSchema{
		Source:       input.Source("test"),
		SchemaString: schema,
	}, RequirePrefixedObjectType())
	require.ErrorContains(t, err, "found reference `user` without prefix")
}

func TestTypeAnnotationNotNormalizedWithoutTypecheckingFlag(t *testing.T) {
	// Without the typechecking flag, annotations are stripped from the compiled
	// schema, so they must not be prefix-normalized (or rejected) either.
	schema := `definition pfx/user {}
definition pfx/group {
	relation member: pfx/user
	permission perm: user = member
}`

	compiled, err := Compile(InputSchema{
		Source:       input.Source("test"),
		SchemaString: schema,
	}, RequirePrefixedObjectType())
	require.NoError(t, err)

	for _, def := range compiled.ObjectDefinitions {
		for _, rel := range def.Relation {
			require.Empty(t, namespace.GetTypeAnnotations(rel))
		}
	}
}

func TestTypeAnnotationInPartialNormalized(t *testing.T) {
	// Partials are translated before the main loop processes `use` directives,
	// so annotation normalization must see the typechecking flag regardless of
	// translation order.
	schema := `use typechecking
use partial

partial view_partial {
	relation viewer: user
	permission view: user = viewer
}

definition user {}

definition document {
	...view_partial
}`

	compiled, err := Compile(InputSchema{
		Source:       input.Source("test"),
		SchemaString: schema,
	}, ObjectTypePrefix("theprefix"))
	require.NoError(t, err)

	annotations := annotationsForPermission(t, compiled, "theprefix/document", "view")
	require.Equal(t, []string{"theprefix/user"}, annotations)
}

func TestTypeAnnotationInImportNormalized(t *testing.T) {
	// Imported definitions are hoisted before the root file's nodes, so they are
	// translated before the root's `use typechecking` directive is reached by
	// the main loop. The imported file deliberately omits its own directive.
	fsys := fstest.MapFS{
		"user.zed": &fstest.MapFile{
			Data: []byte(`definition user {}
definition group {
	relation member: user
	permission perm: user = member
}`),
		},
	}

	schema := `use import
use typechecking

import "./user.zed"

definition document {
	relation viewer: user
	permission view: user = viewer
}`

	compiled, err := Compile(InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schema,
	}, ObjectTypePrefix("theprefix"), SourceFS(fsys))
	require.NoError(t, err)

	require.Equal(t, []string{"theprefix/user"}, annotationsForPermission(t, compiled, "theprefix/group", "perm"))
	require.Equal(t, []string{"theprefix/user"}, annotationsForPermission(t, compiled, "theprefix/document", "view"))
}

func annotationsForPermission(t *testing.T, compiled *CompiledSchema, definition string, permission string) []string {
	t.Helper()

	for _, def := range compiled.ObjectDefinitions {
		if def.Name == definition {
			for _, rel := range def.Relation {
				if rel.Name == permission {
					return namespace.GetTypeAnnotations(rel)
				}
			}
		}
	}

	require.Failf(t, "permission not found", "%s#%s not found in compiled schema", definition, permission)
	return nil
}

func TestTypeAnnotationsHelperFunctions(t *testing.T) {
	tests := []struct {
		name                string
		setupMetadata       func() *core.Metadata
		expectedAnnotations []string
		expectError         bool
	}{
		{
			name: "nil metadata",
			setupMetadata: func() *core.Metadata {
				return nil
			},
			expectedAnnotations: nil,
			expectError:         false,
		},
		{
			name: "empty metadata messages",
			setupMetadata: func() *core.Metadata {
				return &core.Metadata{
					MetadataMessage: []*anypb.Any{},
				}
			},
			expectedAnnotations: nil,
			expectError:         false,
		},
		{
			name: "metadata with non-RelationMetadata message",
			setupMetadata: func() *core.Metadata {
				docComment := &implv1.DocComment{Comment: "test comment"}
				docAny, _ := anypb.New(docComment)
				return &core.Metadata{
					MetadataMessage: []*anypb.Any{docAny},
				}
			},
			expectedAnnotations: nil,
			expectError:         false,
		},
		{
			name: "metadata with RELATION kind (not PERMISSION)",
			setupMetadata: func() *core.Metadata {
				relationMetadata := &implv1.RelationMetadata{
					Kind: implv1.RelationMetadata_RELATION,
					TypeAnnotations: &implv1.TypeAnnotations{
						Types: []string{"ignored"},
					},
				}
				relAny, _ := anypb.New(relationMetadata)
				return &core.Metadata{
					MetadataMessage: []*anypb.Any{relAny},
				}
			},
			expectedAnnotations: nil,
			expectError:         false,
		},
		{
			name: "metadata with PERMISSION kind and type annotations",
			setupMetadata: func() *core.Metadata {
				relationMetadata := &implv1.RelationMetadata{
					Kind: implv1.RelationMetadata_PERMISSION,
					TypeAnnotations: &implv1.TypeAnnotations{
						Types: []string{"user", "admin"},
					},
				}
				relAny, _ := anypb.New(relationMetadata)
				return &core.Metadata{
					MetadataMessage: []*anypb.Any{relAny},
				}
			},
			expectedAnnotations: []string{"user", "admin"},
			expectError:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relation := &core.Relation{
				Name:     "test",
				Metadata: tt.setupMetadata(),
			}

			annotations := namespace.GetTypeAnnotations(relation)
			require.Equal(t, tt.expectedAnnotations, annotations)
		})
	}
}
