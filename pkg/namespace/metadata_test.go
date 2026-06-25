package namespace

import (
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/testutil"
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

	verr := protovalidate.Validate(ns)
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

func TestMixedOperatorsWithoutParens(t *testing.T) {
	position := &core.SourcePosition{ZeroIndexedLineNumber: 3, ZeroIndexedColumnPosition: 17}

	t.Run("get from relation with no metadata", func(t *testing.T) {
		rel := &core.Relation{Name: "test"}
		require.False(t, HasMixedOperatorsWithoutParens(rel))
		require.Nil(t, GetMixedOperatorsPosition(rel))
	})

	t.Run("set false on relation with no metadata is a no-op", func(t *testing.T) {
		rel := &core.Relation{Name: "test"}
		require.NoError(t, SetMixedOperatorsWithoutParens(rel, false, nil))
		require.Nil(t, rel.Metadata)
		require.False(t, HasMixedOperatorsWithoutParens(rel))
	})

	t.Run("set true on relation with no metadata creates permission metadata", func(t *testing.T) {
		rel := &core.Relation{Name: "test"}
		require.NoError(t, SetMixedOperatorsWithoutParens(rel, true, position))
		require.True(t, HasMixedOperatorsWithoutParens(rel))
		testutil.RequireProtoEqual(t, position, GetMixedOperatorsPosition(rel), "mismatched position")
	})

	t.Run("set true updates existing permission metadata in place", func(t *testing.T) {
		relationMetadata := &iv1.RelationMetadata{Kind: iv1.RelationMetadata_PERMISSION}
		metadataAny, err := anypb.New(relationMetadata)
		require.NoError(t, err)
		rel := &core.Relation{
			Name:     "test",
			Metadata: &core.Metadata{MetadataMessage: []*anypb.Any{metadataAny}},
		}

		require.NoError(t, SetMixedOperatorsWithoutParens(rel, true, position))
		require.True(t, HasMixedOperatorsWithoutParens(rel))
		testutil.RequireProtoEqual(t, position, GetMixedOperatorsPosition(rel), "mismatched position")
		require.Len(t, rel.Metadata.MetadataMessage, 1, "should update in place, not append")
	})

	t.Run("set true appends permission metadata when only other metadata exists", func(t *testing.T) {
		docAny, err := anypb.New(&iv1.DocComment{Comment: "test"})
		require.NoError(t, err)
		rel := &core.Relation{
			Name:     "test",
			Metadata: &core.Metadata{MetadataMessage: []*anypb.Any{docAny}},
		}

		require.NoError(t, SetMixedOperatorsWithoutParens(rel, true, position))
		require.True(t, HasMixedOperatorsWithoutParens(rel))
		testutil.RequireProtoEqual(t, position, GetMixedOperatorsPosition(rel), "mismatched position")
		require.Len(t, rel.Metadata.MetadataMessage, 2, "should append a permission metadata entry")
	})

	t.Run("get ignores non-permission metadata", func(t *testing.T) {
		relationMetadata := &iv1.RelationMetadata{Kind: iv1.RelationMetadata_RELATION}
		metadataAny, err := anypb.New(relationMetadata)
		require.NoError(t, err)
		rel := &core.Relation{
			Name:     "test",
			Metadata: &core.Metadata{MetadataMessage: []*anypb.Any{metadataAny}},
		}
		require.False(t, HasMixedOperatorsWithoutParens(rel))
		require.Nil(t, GetMixedOperatorsPosition(rel))
	})
}
