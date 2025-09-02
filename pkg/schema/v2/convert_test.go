package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestConvertDefinitionEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("definition with nil relation", func(t *testing.T) {
		t.Parallel()
		def := &corev1.NamespaceDefinition{
			Name: "test",
			Relation: []*corev1.Relation{
				nil, // This should be handled gracefully
			},
		}

		result, err := convertDefinition(def)
		require.NoError(t, err)
		require.Equal(t, "test", result.Name)
		require.Empty(t, result.Relations)
		require.Empty(t, result.Permissions)
	})

	t.Run("definition with relation having both userset and typeinfo", func(t *testing.T) {
		t.Parallel()
		// This should prioritize userset rewrite over type information
		def := &corev1.NamespaceDefinition{
			Name: "test",
			Relation: []*corev1.Relation{
				{
					Name: "test_rel",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_XThis{},
									},
								},
							},
						},
					},
					TypeInformation: &corev1.TypeInformation{
						AllowedDirectRelations: []*corev1.AllowedRelation{
							{
								Namespace:          "user",
								RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "..."},
							},
						},
					},
				},
			},
		}

		result, err := convertDefinition(def)
		require.NoError(t, err)
		require.Equal(t, "test", result.Name)
		require.Empty(t, result.Relations)
		require.Len(t, result.Permissions, 1)
		require.Contains(t, result.Permissions, "test_rel")
	})
}

func TestConvertUsersetEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil userset", func(t *testing.T) {
		t.Parallel()
		_, err := convertUserset(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "userset rewrite is nil")
	})

	t.Run("unknown rewrite operation", func(t *testing.T) {
		t.Parallel()
		userset := &corev1.UsersetRewrite{
			// No rewrite operation set
		}

		_, err := convertUserset(userset)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown userset rewrite operation type")
	})
}

func TestConvertTypeInformationEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil type information", func(t *testing.T) {
		t.Parallel()
		_, err := convertTypeInformation(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "type information is nil")
	})

	t.Run("type information with caveat and expiration", func(t *testing.T) {
		t.Parallel()
		typeinfo := &corev1.TypeInformation{
			AllowedDirectRelations: []*corev1.AllowedRelation{
				{
					Namespace:          "user",
					RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "..."},
					RequiredCaveat:     &corev1.AllowedCaveat{CaveatName: "test_caveat"},
					RequiredExpiration: &corev1.ExpirationTrait{},
				},
			},
		}

		result, err := convertTypeInformation(typeinfo)
		require.NoError(t, err)
		require.Len(t, result.BaseRelations, 1)

		baseRel := result.BaseRelations[0]
		require.Equal(t, "user", baseRel.Type)
		require.False(t, baseRel.Wildcard)
		require.Equal(t, "test_caveat", baseRel.Caveat)
		require.True(t, baseRel.Expiration)
	})

	t.Run("type information with empty relation name", func(t *testing.T) {
		t.Parallel()
		typeinfo := &corev1.TypeInformation{
			AllowedDirectRelations: []*corev1.AllowedRelation{
				{
					Namespace:          "user",
					RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: ""}, // Empty relation name
				},
			},
		}

		result, err := convertTypeInformation(typeinfo)
		require.NoError(t, err)
		require.Len(t, result.BaseRelations, 1)

		baseRel := result.BaseRelations[0]
		require.Equal(t, "user", baseRel.Type)
		require.Empty(t, baseRel.Subrelation)
	})

	t.Run("type information with ellipsis relation", func(t *testing.T) {
		t.Parallel()
		typeinfo := &corev1.TypeInformation{
			AllowedDirectRelations: []*corev1.AllowedRelation{
				{
					Namespace:          "user",
					RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "..."}, // Ellipsis relation
				},
			},
		}

		result, err := convertTypeInformation(typeinfo)
		require.NoError(t, err)
		require.Len(t, result.BaseRelations, 1)

		baseRel := result.BaseRelations[0]
		require.Equal(t, "user", baseRel.Type)
		require.Empty(t, baseRel.Subrelation)
	})

	t.Run("type information with specific subrelation", func(t *testing.T) {
		t.Parallel()
		typeinfo := &corev1.TypeInformation{
			AllowedDirectRelations: []*corev1.AllowedRelation{
				{
					Namespace:          "organization",
					RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "member"},
				},
			},
		}

		result, err := convertTypeInformation(typeinfo)
		require.NoError(t, err)
		require.Len(t, result.BaseRelations, 1)

		baseRel := result.BaseRelations[0]
		require.Equal(t, "organization", baseRel.Type)
		require.Equal(t, "member", baseRel.Subrelation)
	})
}

func TestConvertSetOperationEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil set operation", func(t *testing.T) {
		t.Parallel()
		_, err := convertSetOperation(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "set operation is nil")
	})

	t.Run("empty set operation", func(t *testing.T) {
		t.Parallel()
		setOp := &corev1.SetOperation{
			Child: []*corev1.SetOperation_Child{},
		}

		result, err := convertSetOperation(setOp)
		require.NoError(t, err)

		// Should create a UnionOperation with empty children
		union, ok := result.(*UnionOperation)
		require.True(t, ok)
		require.Empty(t, union.Children)
	})

	t.Run("single child optimization", func(t *testing.T) {
		t.Parallel()
		setOp := &corev1.SetOperation{
			Child: []*corev1.SetOperation_Child{
				{
					ChildType: &corev1.SetOperation_Child_XThis{},
				},
			},
		}

		result, err := convertSetOperation(setOp)
		require.NoError(t, err)

		// Should return the child directly, not wrapped in UnionOperation
		relRef, ok := result.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "_this", relRef.RelationName)
	})
}

func TestConvertSetOperationAsIntersectionEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil set operation", func(t *testing.T) {
		t.Parallel()
		_, err := convertSetOperationAsIntersection(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "set operation is nil")
	})

	t.Run("single child optimization", func(t *testing.T) {
		t.Parallel()
		setOp := &corev1.SetOperation{
			Child: []*corev1.SetOperation_Child{
				{
					ChildType: &corev1.SetOperation_Child_XThis{},
				},
			},
		}

		result, err := convertSetOperationAsIntersection(setOp)
		require.NoError(t, err)

		// Should return the child directly, not wrapped in IntersectionOperation
		relRef, ok := result.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "_this", relRef.RelationName)
	})
}

func TestConvertSetOperationAsExclusionEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil set operation", func(t *testing.T) {
		t.Parallel()
		_, err := convertSetOperationAsExclusion(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "set operation is nil")
	})

	t.Run("wrong number of children", func(t *testing.T) {
		t.Parallel()
		t.Run("no children", func(t *testing.T) {
			t.Parallel()
			setOp := &corev1.SetOperation{
				Child: []*corev1.SetOperation_Child{},
			}

			_, err := convertSetOperationAsExclusion(setOp)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exclusion operation requires exactly 2 children, got 0")
		})

		t.Run("one child", func(t *testing.T) {
			t.Parallel()
			setOp := &corev1.SetOperation{
				Child: []*corev1.SetOperation_Child{
					{
						ChildType: &corev1.SetOperation_Child_XThis{},
					},
				},
			}

			_, err := convertSetOperationAsExclusion(setOp)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exclusion operation requires exactly 2 children, got 1")
		})

		t.Run("three children", func(t *testing.T) {
			t.Parallel()
			setOp := &corev1.SetOperation{
				Child: []*corev1.SetOperation_Child{
					{
						ChildType: &corev1.SetOperation_Child_XThis{},
					},
					{
						ChildType: &corev1.SetOperation_Child_XThis{},
					},
					{
						ChildType: &corev1.SetOperation_Child_XThis{},
					},
				},
			}

			_, err := convertSetOperationAsExclusion(setOp)
			require.Error(t, err)
			require.Contains(t, err.Error(), "exclusion operation requires exactly 2 children, got 3")
		})
	})

	t.Run("valid exclusion", func(t *testing.T) {
		t.Parallel()
		setOp := &corev1.SetOperation{
			Child: []*corev1.SetOperation_Child{
				{
					ChildType: &corev1.SetOperation_Child_ComputedUserset{
						ComputedUserset: &corev1.ComputedUserset{
							Relation: "left",
						},
					},
				},
				{
					ChildType: &corev1.SetOperation_Child_ComputedUserset{
						ComputedUserset: &corev1.ComputedUserset{
							Relation: "right",
						},
					},
				},
			},
		}

		result, err := convertSetOperationAsExclusion(setOp)
		require.NoError(t, err)

		exclusion, ok := result.(*ExclusionOperation)
		require.True(t, ok)

		leftRef, ok := exclusion.Left.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "left", leftRef.RelationName)

		rightRef, ok := exclusion.Right.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "right", rightRef.RelationName)
	})
}

func TestConvertChildEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil child", func(t *testing.T) {
		t.Parallel()
		_, err := convertChild(nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "child is nil")
	})

	t.Run("XThis child", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_XThis{},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		relRef, ok := result.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "_this", relRef.RelationName)
	})

	t.Run("ComputedUserset child", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_ComputedUserset{
				ComputedUserset: &corev1.ComputedUserset{
					Relation: "test_relation",
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		relRef, ok := result.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "test_relation", relRef.RelationName)
	})

	t.Run("UsersetRewrite child", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_UsersetRewrite{
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Union{
						Union: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_XThis{},
								},
							},
						},
					},
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		// Should return the nested operation directly
		relRef, ok := result.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "_this", relRef.RelationName)
	})

	t.Run("TupleToUserset child", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_TupleToUserset{
				TupleToUserset: &corev1.TupleToUserset{
					Tupleset: &corev1.TupleToUserset_Tupleset{
						Relation: "parent",
					},
					ComputedUserset: &corev1.ComputedUserset{
						Relation: "member",
					},
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		arrow, ok := result.(*ArrowReference)
		require.True(t, ok)
		require.Equal(t, "parent", arrow.Left)
		require.Equal(t, "member", arrow.Right)
	})

	t.Run("XNil child", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_XNil{},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		relRef, ok := result.(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "_nil", relRef.RelationName)
	})

	t.Run("unknown child type", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			// No ChildType set
		}

		_, err := convertChild(child)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown child type")
	})
}

func TestConvertCaveatEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("caveat with empty parameter types", func(t *testing.T) {
		t.Parallel()
		def := &corev1.CaveatDefinition{
			Name:                 "test_caveat",
			SerializedExpression: []byte("test expression"),
			ParameterTypes:       map[string]*corev1.CaveatTypeReference{},
		}

		result, err := convertCaveat(def)
		require.NoError(t, err)
		require.Equal(t, "test_caveat", result.Name)
		require.Equal(t, "test expression", result.Expression)
		require.Empty(t, result.ParameterTypes)
	})

	t.Run("caveat with multiple parameter types", func(t *testing.T) {
		t.Parallel()
		def := &corev1.CaveatDefinition{
			Name:                 "complex_caveat",
			SerializedExpression: []byte("complex expression"),
			ParameterTypes: map[string]*corev1.CaveatTypeReference{
				"param1": {TypeName: "string"},
				"param2": {TypeName: "int"},
				"param3": {TypeName: "bool"},
			},
		}

		result, err := convertCaveat(def)
		require.NoError(t, err)
		require.Equal(t, "complex_caveat", result.Name)
		require.Equal(t, "complex expression", result.Expression)
		require.Len(t, result.ParameterTypes, 3)
		require.Contains(t, result.ParameterTypes, "param1")
		require.Contains(t, result.ParameterTypes, "param2")
		require.Contains(t, result.ParameterTypes, "param3")
	})
}

func TestConvertChildWithNestedOperations(t *testing.T) {
	t.Parallel()

	t.Run("nested union in userset rewrite", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_UsersetRewrite{
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Union{
						Union: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_ComputedUserset{
										ComputedUserset: &corev1.ComputedUserset{
											Relation: "rel1",
										},
									},
								},
								{
									ChildType: &corev1.SetOperation_Child_ComputedUserset{
										ComputedUserset: &corev1.ComputedUserset{
											Relation: "rel2",
										},
									},
								},
							},
						},
					},
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		union, ok := result.(*UnionOperation)
		require.True(t, ok)
		require.Len(t, union.Children, 2)

		rel1, ok := union.Children[0].(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "rel1", rel1.RelationName)

		rel2, ok := union.Children[1].(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "rel2", rel2.RelationName)
	})

	t.Run("nested intersection in userset rewrite", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_UsersetRewrite{
				UsersetRewrite: &corev1.UsersetRewrite{
					RewriteOperation: &corev1.UsersetRewrite_Intersection{
						Intersection: &corev1.SetOperation{
							Child: []*corev1.SetOperation_Child{
								{
									ChildType: &corev1.SetOperation_Child_ComputedUserset{
										ComputedUserset: &corev1.ComputedUserset{
											Relation: "intersect1",
										},
									},
								},
								{
									ChildType: &corev1.SetOperation_Child_ComputedUserset{
										ComputedUserset: &corev1.ComputedUserset{
											Relation: "intersect2",
										},
									},
								},
							},
						},
					},
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		intersection, ok := result.(*IntersectionOperation)
		require.True(t, ok)
		require.Len(t, intersection.Children, 2)
	})

	t.Run("error in nested userset rewrite", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_UsersetRewrite{
				UsersetRewrite: nil, // This should cause an error
			},
		}

		_, err := convertChild(child)
		require.Error(t, err)
		require.Contains(t, err.Error(), "userset rewrite is nil")
	})
}
