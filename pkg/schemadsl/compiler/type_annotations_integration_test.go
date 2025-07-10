package compiler

import (
	"testing"

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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled, err := Compile(InputSchema{
				Source:       input.Source("test"),
				SchemaString: tt.schema,
			}, AllowUnprefixedObjectType())
			require.NoError(t, err)
			require.NotNil(t, compiled)

			// Find the document definition and the expected permission
			var foundPermission *core.Relation
			for _, ns := range compiled.ObjectDefinitions {
				if ns.Name == "document" {
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
