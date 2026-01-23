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

func TestMixedOperatorsMetadata(t *testing.T) {
	tests := []struct {
		name             string
		setupRelation    func() *core.Relation
		performSet       bool
		setMixed         bool
		setPosition      *core.SourcePosition
		expectedHasMixed bool
		expectedPosition *core.SourcePosition
	}{
		{
			name: "has mixed returns false for nil metadata",
			setupRelation: func() *core.Relation {
				return &core.Relation{Name: "test"}
			},
			expectedHasMixed: false,
			expectedPosition: nil,
		},
		{
			name: "has mixed returns false for empty metadata",
			setupRelation: func() *core.Relation {
				return &core.Relation{
					Name:     "test",
					Metadata: &core.Metadata{},
				}
			},
			expectedHasMixed: false,
			expectedPosition: nil,
		},
		{
			name: "has mixed returns false for non-permission relation",
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
			expectedHasMixed: false,
			expectedPosition: nil,
		},
		{
			name: "has mixed returns false for permission without mixed flag",
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
			expectedHasMixed: false,
			expectedPosition: nil,
		},
		{
			name: "has mixed returns true when flag is set",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind:                                iv1.RelationMetadata_PERMISSION,
					HasMixedOperatorsWithoutParentheses: true,
					MixedOperatorsPosition: &core.SourcePosition{
						ZeroIndexedLineNumber:     5,
						ZeroIndexedColumnPosition: 10,
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
			expectedHasMixed: true,
			expectedPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     5,
				ZeroIndexedColumnPosition: 10,
			},
		},
		{
			name: "has mixed returns true with nil position",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind:                                iv1.RelationMetadata_PERMISSION,
					HasMixedOperatorsWithoutParentheses: true,
				}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			expectedHasMixed: true,
			expectedPosition: nil,
		},
		{
			name: "set mixed on relation with no metadata",
			setupRelation: func() *core.Relation {
				return &core.Relation{Name: "test"}
			},
			performSet: true,
			setMixed:   true,
			setPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     3,
				ZeroIndexedColumnPosition: 7,
			},
			expectedHasMixed: true,
			expectedPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     3,
				ZeroIndexedColumnPosition: 7,
			},
		},
		{
			name: "set mixed false on relation with no metadata is no-op",
			setupRelation: func() *core.Relation {
				return &core.Relation{Name: "test"}
			},
			performSet:       true,
			setMixed:         false,
			setPosition:      nil,
			expectedHasMixed: false,
			expectedPosition: nil,
		},
		{
			name: "set mixed updates existing permission metadata",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind: iv1.RelationMetadata_PERMISSION,
				}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{metadataAny},
					},
				}
			},
			performSet: true,
			setMixed:   true,
			setPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     2,
				ZeroIndexedColumnPosition: 15,
			},
			expectedHasMixed: true,
			expectedPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     2,
				ZeroIndexedColumnPosition: 15,
			},
		},
		{
			name: "set mixed on relation with non-permission metadata creates new entry",
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
			performSet: true,
			setMixed:   true,
			setPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     1,
				ZeroIndexedColumnPosition: 5,
			},
			expectedHasMixed: true,
			expectedPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     1,
				ZeroIndexedColumnPosition: 5,
			},
		},
		{
			name: "set mixed false clears existing flag",
			setupRelation: func() *core.Relation {
				relationMetadata := &iv1.RelationMetadata{
					Kind:                                iv1.RelationMetadata_PERMISSION,
					HasMixedOperatorsWithoutParentheses: true,
					MixedOperatorsPosition: &core.SourcePosition{
						ZeroIndexedLineNumber:     5,
						ZeroIndexedColumnPosition: 10,
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
			performSet:       true,
			setMixed:         false,
			setPosition:      nil,
			expectedHasMixed: false,
			expectedPosition: nil,
		},
		{
			name: "metadata with doc comment before relation metadata",
			setupRelation: func() *core.Relation {
				docComment := &iv1.DocComment{Comment: "a comment"}
				docAny, _ := anypb.New(docComment)
				relationMetadata := &iv1.RelationMetadata{
					Kind:                                iv1.RelationMetadata_PERMISSION,
					HasMixedOperatorsWithoutParentheses: true,
					MixedOperatorsPosition: &core.SourcePosition{
						ZeroIndexedLineNumber:     8,
						ZeroIndexedColumnPosition: 20,
					},
				}
				metadataAny, _ := anypb.New(relationMetadata)
				return &core.Relation{
					Name: "test",
					Metadata: &core.Metadata{
						MetadataMessage: []*anypb.Any{docAny, metadataAny},
					},
				}
			},
			expectedHasMixed: true,
			expectedPosition: &core.SourcePosition{
				ZeroIndexedLineNumber:     8,
				ZeroIndexedColumnPosition: 20,
			},
		},
		{
			name: "set mixed false on relation with non-permission metadata only",
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
			performSet:       true,
			setMixed:         false,
			setPosition:      nil,
			expectedHasMixed: false,
			expectedPosition: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relation := tt.setupRelation()

			if tt.performSet {
				err := SetMixedOperatorsWithoutParens(relation, tt.setMixed, tt.setPosition)
				require.NoError(t, err)
			}

			hasMixed := HasMixedOperatorsWithoutParens(relation)
			require.Equal(t, tt.expectedHasMixed, hasMixed)

			position := GetMixedOperatorsPosition(relation)
			if tt.expectedPosition == nil {
				require.Nil(t, position)
			} else {
				require.NotNil(t, position)
				require.Equal(t, tt.expectedPosition.ZeroIndexedLineNumber, position.ZeroIndexedLineNumber)
				require.Equal(t, tt.expectedPosition.ZeroIndexedColumnPosition, position.ZeroIndexedColumnPosition)
			}
		})
	}
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
