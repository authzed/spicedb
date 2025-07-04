package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

func TestMetadata(t *testing.T) {
	require := require.New(t)

	marshalled, err := anypb.New(&iv1.DocComment{
		Comment: "Hi there",
	})
	require.NoError(err)

	marshalledKind, err := anypb.New(&iv1.RelationMetadata{
		Kind: iv1.RelationMetadata_PERMISSION,
	})
	require.NoError(err)

	ns := &core.NamespaceDefinition{
		Name: "somens",
		Relation: []*core.Relation{
			{
				Name: "somerelation",
				Metadata: &core.Metadata{
					MetadataMessage: []*anypb.Any{
						marshalledKind, marshalled,
					},
				},
			},
			{
				Name: "anotherrelation",
			},
		},
		Metadata: &core.Metadata{
			MetadataMessage: []*anypb.Any{
				marshalled,
			},
		},
	}

	verr := ns.Validate()
	require.NoError(verr)

	require.Equal([]string{"Hi there"}, GetComments(ns.Metadata))
	require.Equal([]string{"Hi there"}, GetComments(ns.Relation[0].Metadata))
	require.Equal(iv1.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))

	require.Equal([]string{}, GetComments(ns.Relation[1].Metadata))

	FilterUserDefinedMetadataInPlace(ns)
	require.Equal([]string{}, GetComments(ns.Metadata))
	require.Equal([]string{}, GetComments(ns.Relation[0].Metadata))

	require.Equal(iv1.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))
}

func TestTypeAnnotations(t *testing.T) {
	tests := []struct {
		name                string
		setupRelation       func() *core.Relation
		setAnnotations      []string
		expectedAnnotations []string
		expectError         bool
	}{
		{
			name: "get from relation with no metadata",
			setupRelation: func() *core.Relation {
				return &core.Relation{Name: "test"}
			},
			expectedAnnotations: nil,
		},
		{
			name: "get from relation with empty metadata",
			setupRelation: func() *core.Relation {
				return &core.Relation{
					Name:     "test",
					Metadata: &core.Metadata{},
				}
			},
			expectedAnnotations: nil,
		},
		{
			name: "get from non-permission relation",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{Kind: iv1.RelationMetadata_RELATION}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			expectedAnnotations: nil,
		},
		{
			name: "get from permission relation without type annotations",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{Kind: iv1.RelationMetadata_PERMISSION}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			expectedAnnotations: nil,
		},
		{
			name: "get from permission relation with type annotations",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind: iv1.RelationMetadata_PERMISSION,
					TypeAnnotations: &iv1.TypeAnnotations{
						Types: []string{"user", "group"},
					},
				}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			expectedAnnotations: []string{"user", "group"},
		},
		{
			name: "set on relation with no metadata",
			setupRelation: func() *core.Relation {
				return &core.Relation{Name: "test"}
			},
			setAnnotations:      []string{"user", "group"},
			expectedAnnotations: []string{"user", "group"},
		},
		{
			name: "set on relation with existing metadata",
			setupRelation: func() *core.Relation {
				docComment := &iv1.DocComment{Comment: "test comment"}
				docAny, _ := anypb.New(docComment)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{docAny},
					},
				}
			},
			setAnnotations:      []string{"user"},
			expectedAnnotations: []string{"user"},
		},
		{
			name: "update existing permission metadata",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind: iv1.RelationMetadata_PERMISSION,
					TypeAnnotations: &iv1.TypeAnnotations{
						Types: []string{"user"},
					},
				}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			setAnnotations:      []string{"user", "group", "organization"},
			expectedAnnotations: []string{"user", "group", "organization"},
		},
		{
			name: "remove annotations with nil",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind: iv1.RelationMetadata_PERMISSION,
					TypeAnnotations: &iv1.TypeAnnotations{
						Types: []string{"user", "group"},
					},
				}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			setAnnotations:      nil,
			expectedAnnotations: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relation := tt.setupRelation()

			if tt.setAnnotations != nil || (tt.setAnnotations == nil && tt.name == "remove annotations with nil") {
				err := SetTypeAnnotations(relation, tt.setAnnotations)
				if tt.expectError {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			annotations := GetTypeAnnotations(relation)
			require.Equal(t, tt.expectedAnnotations, annotations)
		})
	}
}
