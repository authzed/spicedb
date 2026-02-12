package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
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
		require.Equal(t, "test", result.Name())
		require.Empty(t, result.Relations())
		require.Empty(t, result.Permissions())
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
		require.Equal(t, "test", result.Name())
		require.Empty(t, result.Relations())
		require.Len(t, result.Permissions(), 1)
		require.Contains(t, result.Permissions(), "test_rel")
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
		require.Len(t, result.BaseRelations(), 1)

		baseRel := result.BaseRelations()[0]
		require.Equal(t, "user", baseRel.Type())
		require.False(t, baseRel.Wildcard())
		require.Equal(t, "test_caveat", baseRel.Caveat())
		require.True(t, baseRel.Expiration())
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
		require.Len(t, result.BaseRelations(), 1)

		baseRel := result.BaseRelations()[0]
		require.Equal(t, "user", baseRel.Type())
		require.Equal(t, "...", baseRel.Subrelation())
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
		require.Len(t, result.BaseRelations(), 1)

		baseRel := result.BaseRelations()[0]
		require.Equal(t, "user", baseRel.Type())
		require.Equal(t, "...", baseRel.Subrelation())
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
		require.Len(t, result.BaseRelations(), 1)

		baseRel := result.BaseRelations()[0]
		require.Equal(t, "organization", baseRel.Type())
		require.Equal(t, "member", baseRel.Subrelation())
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
		require.Empty(t, union.Children())
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
		require.Equal(t, "_this", relRef.RelationName())
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
		require.Equal(t, "_this", relRef.RelationName())
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

		leftRef, ok := exclusion.Left().(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "left", leftRef.RelationName())

		rightRef, ok := exclusion.Right().(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "right", rightRef.RelationName())
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
		require.Equal(t, "_this", relRef.RelationName())
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
		require.Equal(t, "test_relation", relRef.RelationName())
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
		require.Equal(t, "_this", relRef.RelationName())
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
		require.Equal(t, "parent", arrow.Left())
		require.Equal(t, "member", arrow.Right())
	})

	t.Run("XNil child", func(t *testing.T) {
		t.Parallel()
		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_XNil{},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		nilRef, ok := result.(*NilReference)
		require.True(t, ok)
		require.NotNil(t, nilRef)
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
		require.Equal(t, "test_caveat", result.Name())
		require.Equal(t, "test expression", result.Expression())
		require.Empty(t, result.Parameters())
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
		require.Equal(t, "complex_caveat", result.Name())
		require.Equal(t, "complex expression", result.Expression())
		require.Len(t, result.Parameters(), 3)

		// Check that all parameters are present
		paramMap := make(map[string]string)
		for _, param := range result.Parameters() {
			paramMap[param.Name()] = param.Type()
		}
		require.Equal(t, "string", paramMap["param1"])
		require.Equal(t, "int", paramMap["param2"])
		require.Equal(t, "bool", paramMap["param3"])
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
		require.Len(t, union.Children(), 2)

		rel1, ok := union.Children()[0].(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "rel1", rel1.RelationName())

		rel2, ok := union.Children()[1].(*RelationReference)
		require.True(t, ok)
		require.Equal(t, "rel2", rel2.RelationName())
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
		require.Len(t, intersection.Children(), 2)
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

func TestConvertFunctionedTupleToUserset(t *testing.T) {
	t.Parallel()

	t.Run("convert FUNCTION_ANY", func(t *testing.T) {
		t.Parallel()

		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_FunctionedTupleToUserset{
				FunctionedTupleToUserset: &corev1.FunctionedTupleToUserset{
					Function: corev1.FunctionedTupleToUserset_FUNCTION_ANY,
					Tupleset: &corev1.FunctionedTupleToUserset_Tupleset{
						Relation: "team",
					},
					ComputedUserset: &corev1.ComputedUserset{
						Relation: "member",
					},
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		funcOp, ok := result.(*FunctionedArrowReference)
		require.True(t, ok, "Should convert to FunctionedArrowReference")
		require.Equal(t, "team", funcOp.Left())
		require.Equal(t, "member", funcOp.Right())
		require.Equal(t, FunctionTypeAny, funcOp.Function())
	})

	t.Run("convert FUNCTION_ALL", func(t *testing.T) {
		t.Parallel()

		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_FunctionedTupleToUserset{
				FunctionedTupleToUserset: &corev1.FunctionedTupleToUserset{
					Function: corev1.FunctionedTupleToUserset_FUNCTION_ALL,
					Tupleset: &corev1.FunctionedTupleToUserset_Tupleset{
						Relation: "group",
					},
					ComputedUserset: &corev1.ComputedUserset{
						Relation: "viewer",
					},
				},
			},
		}

		result, err := convertChild(child)
		require.NoError(t, err)

		funcOp, ok := result.(*FunctionedArrowReference)
		require.True(t, ok, "Should convert to FunctionedArrowReference")
		require.Equal(t, "group", funcOp.Left())
		require.Equal(t, "viewer", funcOp.Right())
		require.Equal(t, FunctionTypeAll, funcOp.Function())
	})

	t.Run("convert unknown function type", func(t *testing.T) {
		t.Parallel()

		child := &corev1.SetOperation_Child{
			ChildType: &corev1.SetOperation_Child_FunctionedTupleToUserset{
				FunctionedTupleToUserset: &corev1.FunctionedTupleToUserset{
					Function: corev1.FunctionedTupleToUserset_FUNCTION_UNSPECIFIED,
					Tupleset: &corev1.FunctionedTupleToUserset_Tupleset{
						Relation: "team",
					},
					ComputedUserset: &corev1.ComputedUserset{
						Relation: "member",
					},
				},
			},
		}

		_, err := convertChild(child)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown function type")
	})

	t.Run("full definition with functioned tupleset", func(t *testing.T) {
		t.Parallel()

		def := &corev1.NamespaceDefinition{
			Name: "document",
			Relation: []*corev1.Relation{
				// Add a relation for the tupleset
				{
					Name: "team",
					TypeInformation: &corev1.TypeInformation{
						AllowedDirectRelations: []*corev1.AllowedRelation{
							{
								Namespace: "team",
								RelationOrWildcard: &corev1.AllowedRelation_Relation{
									Relation: "",
								},
							},
						},
					},
				},
				// Add a permission with functioned tupleset
				{
					Name: "view",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_FunctionedTupleToUserset{
											FunctionedTupleToUserset: &corev1.FunctionedTupleToUserset{
												Function: corev1.FunctionedTupleToUserset_FUNCTION_ALL,
												Tupleset: &corev1.FunctionedTupleToUserset_Tupleset{
													Relation: "team",
												},
												ComputedUserset: &corev1.ComputedUserset{
													Relation: "member",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		result, err := convertDefinition(def)
		require.NoError(t, err)
		require.Equal(t, "document", result.Name())

		// Check that we have both relation and permission
		require.Len(t, result.Relations(), 1)
		require.Len(t, result.Permissions(), 1)

		// Check the relation
		teamRel, ok := result.Relations()["team"]
		require.True(t, ok)
		require.Equal(t, "team", teamRel.Name())

		// Check the permission with functioned tupleset
		viewPerm, ok := result.Permissions()["view"]
		require.True(t, ok)
		require.Equal(t, "view", viewPerm.Name())

		// The permission should have a FunctionedArrowReference (might be unwrapped from union if single child)
		// Check if it's directly a FunctionedArrowReference or wrapped in a union
		if funcOp, ok := viewPerm.Operation().(*FunctionedArrowReference); ok {
			// Direct operation (unwrapped by optimization)
			require.Equal(t, "team", funcOp.Left())
			require.Equal(t, "member", funcOp.Right())
			require.Equal(t, FunctionTypeAll, funcOp.Function())
		} else if union, ok := viewPerm.Operation().(*UnionOperation); ok {
			// Wrapped in union
			require.Len(t, union.Children(), 1)
			funcOp, ok := union.Children()[0].(*FunctionedArrowReference)
			require.True(t, ok)
			require.Equal(t, "team", funcOp.Left())
			require.Equal(t, "member", funcOp.Right())
			require.Equal(t, FunctionTypeAll, funcOp.Function())
		} else {
			require.Fail(t, "Expected FunctionedArrowReference, got:", viewPerm.Operation())
		}
	})
}

func TestConvertFunctionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    corev1.FunctionedTupleToUserset_Function
		expected FunctionType
		wantErr  bool
	}{
		{
			name:     "FUNCTION_ANY",
			input:    corev1.FunctionedTupleToUserset_FUNCTION_ANY,
			expected: FunctionTypeAny,
			wantErr:  false,
		},
		{
			name:     "FUNCTION_ALL",
			input:    corev1.FunctionedTupleToUserset_FUNCTION_ALL,
			expected: FunctionTypeAll,
			wantErr:  false,
		},
		{
			name:     "FUNCTION_UNSPECIFIED",
			input:    corev1.FunctionedTupleToUserset_FUNCTION_UNSPECIFIED,
			expected: 0,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := convertFunctionType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "unknown function type")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestMetadataPreservation(t *testing.T) {
	t.Parallel()

	t.Run("namespace definition metadata with doc comments is preserved", func(t *testing.T) {
		t.Parallel()

		// Create doc comment metadata
		docComment := &implv1.DocComment{
			Comment: "This is a test resource",
		}
		docCommentAny, err := anypb.New(docComment)
		require.NoError(t, err)

		originalMetadata := &corev1.Metadata{
			MetadataMessage: []*anypb.Any{docCommentAny},
		}

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
				},
			},
			Metadata: originalMetadata,
		}

		// Convert to v2 schema
		schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{originalDef}, nil)
		require.NoError(t, err)

		// Verify internal representation
		def, ok := schema.Definitions()["test_resource"]
		require.True(t, ok)
		require.NotNil(t, def.metadata)
		require.Equal(t, []string{"This is a test resource"}, def.metadata.Comments())

		// Convert back to corev1
		defs, caveats, err := schema.ToDefinitions()
		require.NoError(t, err)
		require.Len(t, defs, 1)
		require.Empty(t, caveats)

		// Verify metadata is preserved
		require.NotNil(t, defs[0].Metadata, "Metadata should be preserved during round-trip conversion")
		require.Len(t, defs[0].Metadata.MetadataMessage, 1)

		// Verify doc comment
		var roundTrippedComment implv1.DocComment
		err = defs[0].Metadata.MetadataMessage[0].UnmarshalTo(&roundTrippedComment)
		require.NoError(t, err)
		require.Equal(t, "This is a test resource", roundTrippedComment.Comment)
	})

	t.Run("caveat definition metadata with doc comments is preserved", func(t *testing.T) {
		t.Parallel()

		// Create doc comment metadata
		docComment := &implv1.DocComment{
			Comment: "This is a test caveat",
		}
		docCommentAny, err := anypb.New(docComment)
		require.NoError(t, err)

		originalMetadata := &corev1.Metadata{
			MetadataMessage: []*anypb.Any{docCommentAny},
		}

		originalCaveat := &corev1.CaveatDefinition{
			Name:                 "test_caveat",
			SerializedExpression: []byte("x == 1"),
			ParameterTypes: map[string]*corev1.CaveatTypeReference{
				"x": {
					TypeName: "int",
				},
			},
			Metadata: originalMetadata,
		}

		// Convert to v2 schema
		schema, err := BuildSchemaFromDefinitions(nil, []*corev1.CaveatDefinition{originalCaveat})
		require.NoError(t, err)

		// Verify internal representation
		caveat, ok := schema.Caveats()["test_caveat"]
		require.True(t, ok)
		require.NotNil(t, caveat.metadata)
		require.Equal(t, []string{"This is a test caveat"}, caveat.metadata.Comments())

		// Convert back to corev1
		defs, caveats, err := schema.ToDefinitions()
		require.NoError(t, err)
		require.Empty(t, defs)
		require.Len(t, caveats, 1)

		// Verify metadata is preserved
		require.NotNil(t, caveats[0].Metadata, "Caveat metadata should be preserved during round-trip conversion")

		// Verify doc comment
		var roundTrippedComment implv1.DocComment
		err = caveats[0].Metadata.MetadataMessage[0].UnmarshalTo(&roundTrippedComment)
		require.NoError(t, err)
		require.Equal(t, "This is a test caveat", roundTrippedComment.Comment)
	})

	t.Run("relation metadata with doc comments and relation kind is preserved", func(t *testing.T) {
		t.Parallel()

		// Create doc comment metadata
		docComment := &implv1.DocComment{
			Comment: "This is a viewer relation",
		}
		docCommentAny, err := anypb.New(docComment)
		require.NoError(t, err)

		// Create relation metadata
		relationMetadata := &implv1.RelationMetadata{
			Kind: implv1.RelationMetadata_RELATION,
		}
		relationMetadataAny, err := anypb.New(relationMetadata)
		require.NoError(t, err)

		combinedMetadata := &corev1.Metadata{
			MetadataMessage: []*anypb.Any{docCommentAny, relationMetadataAny},
		}

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
					Metadata: combinedMetadata,
				},
			},
		}

		// Convert to v2 schema
		schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{originalDef}, nil)
		require.NoError(t, err)

		// Verify internal representation
		def, ok := schema.Definitions()["test_resource"]
		require.True(t, ok)
		rel, ok := def.Relations()["viewer"]
		require.True(t, ok)
		require.NotNil(t, rel.metadata)
		require.Equal(t, []string{"This is a viewer relation"}, rel.metadata.Comments())
		require.Equal(t, RelationKindRelation, rel.metadata.RelationKind())

		// Convert back to corev1
		defs, _, err := schema.ToDefinitions()
		require.NoError(t, err)
		require.Len(t, defs, 1)
		require.Len(t, defs[0].Relation, 1)

		// Verify relation metadata is preserved
		require.NotNil(t, defs[0].Relation[0].Metadata, "Relation metadata should be preserved during round-trip conversion")
		require.Len(t, defs[0].Relation[0].Metadata.MetadataMessage, 2)

		// Verify both doc comment and relation metadata are preserved
		var foundDocComment, foundRelationMetadata bool
		for _, msg := range defs[0].Relation[0].Metadata.MetadataMessage {
			var dc implv1.DocComment
			if err := msg.UnmarshalTo(&dc); err == nil {
				require.Equal(t, "This is a viewer relation", dc.Comment)
				foundDocComment = true
				continue
			}

			var rm implv1.RelationMetadata
			if err := msg.UnmarshalTo(&rm); err == nil {
				require.Equal(t, implv1.RelationMetadata_RELATION, rm.Kind)
				foundRelationMetadata = true
			}
		}

		require.True(t, foundDocComment, "Doc comment should be preserved")
		require.True(t, foundRelationMetadata, "Relation metadata should be preserved")
	})

	t.Run("permission metadata with type annotations is preserved", func(t *testing.T) {
		t.Parallel()

		// Create permission metadata with type annotations
		permissionMetadata := &implv1.RelationMetadata{
			Kind: implv1.RelationMetadata_PERMISSION,
			TypeAnnotations: &implv1.TypeAnnotations{
				Types: []string{"user", "group"},
			},
		}
		permissionMetadataAny, err := anypb.New(permissionMetadata)
		require.NoError(t, err)

		metadata := &corev1.Metadata{
			MetadataMessage: []*anypb.Any{permissionMetadataAny},
		}

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
					Metadata: metadata,
				},
			},
		}

		// Convert to v2 schema
		schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{originalDef}, nil)
		require.NoError(t, err)

		// Verify internal representation
		def, ok := schema.Definitions()["test_resource"]
		require.True(t, ok)
		rel, ok := def.Relations()["viewer"]
		require.True(t, ok)
		require.NotNil(t, rel.metadata)
		require.Equal(t, RelationKindPermission, rel.metadata.RelationKind())
		require.Equal(t, []string{"user", "group"}, rel.metadata.TypeAnnotations())

		// Convert back to corev1
		defs, _, err := schema.ToDefinitions()
		require.NoError(t, err)
		require.Len(t, defs, 1)
		require.Len(t, defs[0].Relation, 1)

		// Verify permission metadata with type annotations is preserved
		require.NotNil(t, defs[0].Relation[0].Metadata)

		var rm implv1.RelationMetadata
		err = defs[0].Relation[0].Metadata.MetadataMessage[0].UnmarshalTo(&rm)
		require.NoError(t, err)
		require.Equal(t, implv1.RelationMetadata_PERMISSION, rm.Kind)
		require.NotNil(t, rm.TypeAnnotations)
		require.Equal(t, []string{"user", "group"}, rm.TypeAnnotations.Types)
	})
}

// TestConvertOperationParentPointers verifies that parent pointers are correctly set
// when operations are converted from proto definitions.
func TestConvertOperationParentPointers(t *testing.T) {
	t.Parallel()

	t.Run("comprehensive parent pointer verification", func(t *testing.T) {
		t.Parallel()

		// Create a comprehensive proto definition that exercises all operation types and parent relationships
		def := &corev1.NamespaceDefinition{
			Name: "document",
			Relation: []*corev1.Relation{
				// Relation with type information - tests Relation and BaseRelation parent pointers
				{
					Name: "owner",
					TypeInformation: &corev1.TypeInformation{
						AllowedDirectRelations: []*corev1.AllowedRelation{
							{
								Namespace:          "user",
								RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "..."},
							},
							{
								Namespace:          "team",
								RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "member"},
							},
						},
					},
				},
				// Relation for arrow references
				{
					Name: "parent",
					TypeInformation: &corev1.TypeInformation{
						AllowedDirectRelations: []*corev1.AllowedRelation{
							{
								Namespace:          "folder",
								RelationOrWildcard: &corev1.AllowedRelation_Relation{Relation: "..."},
							},
						},
					},
				},
				// Permission with nested operations: ((a | b) & c) - (d)
				// This tests Union, Intersection, Exclusion, and RelationReference
				{
					Name: "complex_view",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Exclusion{
							Exclusion: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									// Left side: (a | b) & c
									{
										ChildType: &corev1.SetOperation_Child_UsersetRewrite{
											UsersetRewrite: &corev1.UsersetRewrite{
												RewriteOperation: &corev1.UsersetRewrite_Intersection{
													Intersection: &corev1.SetOperation{
														Child: []*corev1.SetOperation_Child{
															// Nested union: a | b
															{
																ChildType: &corev1.SetOperation_Child_UsersetRewrite{
																	UsersetRewrite: &corev1.UsersetRewrite{
																		RewriteOperation: &corev1.UsersetRewrite_Union{
																			Union: &corev1.SetOperation{
																				Child: []*corev1.SetOperation_Child{
																					{
																						ChildType: &corev1.SetOperation_Child_ComputedUserset{
																							ComputedUserset: &corev1.ComputedUserset{
																								Relation: "a",
																							},
																						},
																					},
																					{
																						ChildType: &corev1.SetOperation_Child_ComputedUserset{
																							ComputedUserset: &corev1.ComputedUserset{
																								Relation: "b",
																							},
																						},
																					},
																				},
																			},
																		},
																	},
																},
															},
															// c
															{
																ChildType: &corev1.SetOperation_Child_ComputedUserset{
																	ComputedUserset: &corev1.ComputedUserset{
																		Relation: "c",
																	},
																},
															},
														},
													},
												},
											},
										},
									},
									// Right side (excluded): d
									{
										ChildType: &corev1.SetOperation_Child_ComputedUserset{
											ComputedUserset: &corev1.ComputedUserset{
												Relation: "d",
											},
										},
									},
								},
							},
						},
					},
				},
				// Permission with arrow reference (TupleToUserset)
				{
					Name: "arrow_view",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_TupleToUserset{
											TupleToUserset: &corev1.TupleToUserset{
												Tupleset: &corev1.TupleToUserset_Tupleset{
													Relation: "parent",
												},
												ComputedUserset: &corev1.ComputedUserset{
													Relation: "view",
												},
											},
										},
									},
								},
							},
						},
					},
				},
				// Permission with functioned arrow reference
				{
					Name: "functioned_view",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_FunctionedTupleToUserset{
											FunctionedTupleToUserset: &corev1.FunctionedTupleToUserset{
												Function: corev1.FunctionedTupleToUserset_FUNCTION_ALL,
												Tupleset: &corev1.FunctionedTupleToUserset_Tupleset{
													Relation: "parent",
												},
												ComputedUserset: &corev1.ComputedUserset{
													Relation: "viewer",
												},
											},
										},
									},
								},
							},
						},
					},
				},
				// Permission with _this (SelfReference) and nil
				{
					Name: "special_refs",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_XThis{},
									},
									{
										ChildType: &corev1.SetOperation_Child_XNil{},
									},
								},
							},
						},
					},
				},
			},
		}

		// Convert to v2 schema - this should build the complete hierarchy
		schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{def}, nil)
		require.NoError(t, err)
		require.NotNil(t, schema)

		// 1. Verify Definition → Schema parent pointer
		docDef, ok := schema.GetTypeDefinition("document")
		require.True(t, ok, "document definition should exist")
		require.NotNil(t, docDef.Parent(), "Definition should have a parent")
		require.Equal(t, schema, docDef.Parent(), "Definition's parent should be the schema")

		// 2. Verify Relation → Definition parent pointer and BaseRelation → Relation parent pointers
		ownerRel, ok := docDef.GetRelation("owner")
		require.True(t, ok, "owner relation should exist")
		require.NotNil(t, ownerRel.Parent(), "Relation should have a parent")
		require.Equal(t, docDef, ownerRel.Parent(), "Relation's parent should be the definition")

		baseRels := ownerRel.BaseRelations()
		require.Len(t, baseRels, 2, "owner should have 2 base relations")
		for i, baseRel := range baseRels {
			require.NotNil(t, baseRel.Parent(), "BaseRelation %d should have a parent", i)
			require.Equal(t, ownerRel, baseRel.Parent(), "BaseRelation %d's parent should be the relation", i)
		}

		// 3. Verify Permission → Definition parent pointer
		complexViewPerm, ok := docDef.GetPermission("complex_view")
		require.True(t, ok, "complex_view permission should exist")
		require.NotNil(t, complexViewPerm.Parent(), "Permission should have a parent")
		require.Equal(t, docDef, complexViewPerm.Parent(), "Permission's parent should be the definition")

		// 4. Verify ExclusionOperation and its children's parent pointers
		rootOp := complexViewPerm.Operation()
		require.NotNil(t, rootOp, "Permission should have an operation")
		require.NotNil(t, rootOp.Parent(), "Root operation should have a parent")
		require.Equal(t, complexViewPerm, rootOp.Parent(), "Root operation's parent should be the permission")

		exclusion, ok := rootOp.(*ExclusionOperation)
		require.True(t, ok, "Root operation should be ExclusionOperation")

		leftOp := exclusion.Left()
		require.NotNil(t, leftOp, "Exclusion left should not be nil")
		require.NotNil(t, leftOp.Parent(), "Exclusion left should have a parent")
		require.Equal(t, exclusion, leftOp.Parent(), "Exclusion left's parent should be the exclusion")

		rightOp := exclusion.Right()
		require.NotNil(t, rightOp, "Exclusion right should not be nil")
		require.NotNil(t, rightOp.Parent(), "Exclusion right should have a parent")
		require.Equal(t, exclusion, rightOp.Parent(), "Exclusion right's parent should be the exclusion")

		// 5. Verify IntersectionOperation and its children's parent pointers
		intersection, ok := leftOp.(*IntersectionOperation)
		require.True(t, ok, "Exclusion left should be IntersectionOperation")

		intersectionChildren := intersection.Children()
		require.Len(t, intersectionChildren, 2, "Intersection should have 2 children")
		for i, child := range intersectionChildren {
			require.NotNil(t, child, "Intersection child %d should not be nil", i)
			require.NotNil(t, child.Parent(), "Intersection child %d should have a parent", i)
			require.Equal(t, intersection, child.Parent(), "Intersection child %d's parent should be the intersection", i)
		}

		// 6. Verify UnionOperation and its children's parent pointers
		union, ok := intersectionChildren[0].(*UnionOperation)
		require.True(t, ok, "First intersection child should be UnionOperation")

		unionChildren := union.Children()
		require.Len(t, unionChildren, 2, "Union should have 2 children")
		for i, child := range unionChildren {
			require.NotNil(t, child, "Union child %d should not be nil", i)
			require.NotNil(t, child.Parent(), "Union child %d should have a parent", i)
			require.Equal(t, union, child.Parent(), "Union child %d's parent should be the union", i)

			// Verify RelationReference
			relRef, ok := child.(*RelationReference)
			require.True(t, ok, "Union child %d should be RelationReference", i)
			require.NotEmpty(t, relRef.RelationName(), "RelationReference should have a relation name")
		}

		// 7. Verify ArrowReference parent pointer
		arrowViewPerm, ok := docDef.GetPermission("arrow_view")
		require.True(t, ok, "arrow_view permission should exist")

		arrowOp := arrowViewPerm.Operation()
		require.NotNil(t, arrowOp, "arrow_view should have an operation")
		require.NotNil(t, arrowOp.Parent(), "arrow_view root operation should have a parent")
		require.Equal(t, arrowViewPerm, arrowOp.Parent(), "arrow_view root operation's parent should be the permission")

		// The operation might be unwrapped if it's a single child, so check both cases
		var arrow *ArrowReference
		if a, ok := arrowOp.(*ArrowReference); ok {
			arrow = a
		} else if union, ok := arrowOp.(*UnionOperation); ok {
			require.Len(t, union.Children(), 1, "Union should have 1 child")
			arrow, ok = union.Children()[0].(*ArrowReference)
			require.True(t, ok, "Union child should be ArrowReference")
			require.NotNil(t, arrow.Parent(), "ArrowReference should have a parent")
			require.Equal(t, union, arrow.Parent(), "ArrowReference's parent should be the union")
		} else {
			require.Fail(t, "Expected ArrowReference or UnionOperation with ArrowReference child")
		}

		require.Equal(t, "parent", arrow.Left(), "Arrow left should be 'parent'")
		require.Equal(t, "view", arrow.Right(), "Arrow right should be 'view'")

		// 8. Verify FunctionedArrowReference parent pointer
		functionedViewPerm, ok := docDef.GetPermission("functioned_view")
		require.True(t, ok, "functioned_view permission should exist")

		functionedOp := functionedViewPerm.Operation()
		require.NotNil(t, functionedOp, "functioned_view should have an operation")
		require.NotNil(t, functionedOp.Parent(), "functioned_view root operation should have a parent")
		require.Equal(t, functionedViewPerm, functionedOp.Parent(), "functioned_view root operation's parent should be the permission")

		var funcArrow *FunctionedArrowReference
		if fa, ok := functionedOp.(*FunctionedArrowReference); ok {
			funcArrow = fa
		} else if union, ok := functionedOp.(*UnionOperation); ok {
			require.Len(t, union.Children(), 1, "Union should have 1 child")
			funcArrow, ok = union.Children()[0].(*FunctionedArrowReference)
			require.True(t, ok, "Union child should be FunctionedArrowReference")
			require.NotNil(t, funcArrow.Parent(), "FunctionedArrowReference should have a parent")
			require.Equal(t, union, funcArrow.Parent(), "FunctionedArrowReference's parent should be the union")
		} else {
			require.Fail(t, "Expected FunctionedArrowReference or UnionOperation with FunctionedArrowReference child")
		}

		require.Equal(t, "parent", funcArrow.Left(), "FunctionedArrow left should be 'parent'")
		require.Equal(t, "viewer", funcArrow.Right(), "FunctionedArrow right should be 'viewer'")
		require.Equal(t, FunctionTypeAll, funcArrow.Function(), "FunctionedArrow should have FUNCTION_ALL")

		// 9. Verify SelfReference and NilReference parent pointers
		specialRefsPerm, ok := docDef.GetPermission("special_refs")
		require.True(t, ok, "special_refs permission should exist")

		specialOp := specialRefsPerm.Operation()
		require.NotNil(t, specialOp, "special_refs should have an operation")
		require.NotNil(t, specialOp.Parent(), "special_refs root operation should have a parent")

		specialUnion, ok := specialOp.(*UnionOperation)
		require.True(t, ok, "special_refs operation should be UnionOperation")

		specialChildren := specialUnion.Children()
		require.Len(t, specialChildren, 2, "special_refs union should have 2 children")

		// Check for SelfReference (from _this)
		selfRef, ok := specialChildren[0].(*RelationReference)
		require.True(t, ok, "First child should be RelationReference (converted from XThis)")
		require.Equal(t, "_this", selfRef.RelationName(), "Should be _this reference")
		require.NotNil(t, selfRef.Parent(), "SelfReference should have a parent")
		require.Equal(t, specialUnion, selfRef.Parent(), "SelfReference's parent should be the union")

		// Check for NilReference
		nilRef, ok := specialChildren[1].(*NilReference)
		require.True(t, ok, "Second child should be NilReference")
		require.NotNil(t, nilRef.Parent(), "NilReference should have a parent")
		require.Equal(t, specialUnion, nilRef.Parent(), "NilReference's parent should be the union")
	})

	t.Run("deeply nested operations maintain parent chain", func(t *testing.T) {
		t.Parallel()

		// Create a deeply nested structure to verify the entire parent chain
		// Use 2 children at the root union to avoid single-child optimization
		def := &corev1.NamespaceDefinition{
			Name: "resource",
			Relation: []*corev1.Relation{
				{
					Name: "deep",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_UsersetRewrite{
											UsersetRewrite: &corev1.UsersetRewrite{
												RewriteOperation: &corev1.UsersetRewrite_Intersection{
													Intersection: &corev1.SetOperation{
														Child: []*corev1.SetOperation_Child{
															{
																ChildType: &corev1.SetOperation_Child_UsersetRewrite{
																	UsersetRewrite: &corev1.UsersetRewrite{
																		RewriteOperation: &corev1.UsersetRewrite_Exclusion{
																			Exclusion: &corev1.SetOperation{
																				Child: []*corev1.SetOperation_Child{
																					{
																						ChildType: &corev1.SetOperation_Child_ComputedUserset{
																							ComputedUserset: &corev1.ComputedUserset{
																								Relation: "leaf",
																							},
																						},
																					},
																					{
																						ChildType: &corev1.SetOperation_Child_ComputedUserset{
																							ComputedUserset: &corev1.ComputedUserset{
																								Relation: "other",
																							},
																						},
																					},
																				},
																			},
																		},
																	},
																},
															},
															{
																ChildType: &corev1.SetOperation_Child_ComputedUserset{
																	ComputedUserset: &corev1.ComputedUserset{
																		Relation: "middle",
																	},
																},
															},
														},
													},
												},
											},
										},
									},
									// Add a second child to avoid single-child optimization
									{
										ChildType: &corev1.SetOperation_Child_ComputedUserset{
											ComputedUserset: &corev1.ComputedUserset{
												Relation: "sibling",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{def}, nil)
		require.NoError(t, err)

		resDef, ok := schema.GetTypeDefinition("resource")
		require.True(t, ok)

		deepPerm, ok := resDef.GetPermission("deep")
		require.True(t, ok)

		// Navigate down the tree and verify parent chain at each level
		rootUnion, ok := deepPerm.Operation().(*UnionOperation)
		require.True(t, ok, "Root should be UnionOperation (not optimized away since it has 2 children)")
		require.Equal(t, deepPerm, rootUnion.Parent())

		intersection, ok := rootUnion.Children()[0].(*IntersectionOperation)
		require.True(t, ok)
		require.Equal(t, rootUnion, intersection.Parent())

		exclusion, ok := intersection.Children()[0].(*ExclusionOperation)
		require.True(t, ok)
		require.Equal(t, intersection, exclusion.Parent())

		leftLeaf, ok := exclusion.Left().(*RelationReference)
		require.True(t, ok)
		require.Equal(t, exclusion, leftLeaf.Parent())
		require.Equal(t, "leaf", leftLeaf.RelationName())

		// Traverse up from the leaf to verify we can reach the root
		current := leftLeaf.Parent()
		require.Equal(t, exclusion, current, "First parent should be exclusion")

		current = current.Parent()
		require.Equal(t, intersection, current, "Second parent should be intersection")

		current = current.Parent()
		require.Equal(t, rootUnion, current, "Third parent should be root union")

		current = current.Parent()
		require.Equal(t, deepPerm, current, "Fourth parent should be permission")

		current = current.Parent()
		require.Equal(t, resDef, current, "Fifth parent should be definition")

		current = current.Parent()
		require.Equal(t, schema, current, "Sixth parent should be schema")

		current = current.Parent()
		require.Nil(t, current, "Schema should have no parent (top of hierarchy)")
	})

	t.Run("single child optimization preserves parent pointers", func(t *testing.T) {
		t.Parallel()

		// Test that single-child optimization (unwrapping) still maintains parent pointers
		def := &corev1.NamespaceDefinition{
			Name: "doc",
			Relation: []*corev1.Relation{
				{
					Name: "single_union",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Union{
							Union: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_ComputedUserset{
											ComputedUserset: &corev1.ComputedUserset{
												Relation: "only_child",
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Name: "single_intersection",
					UsersetRewrite: &corev1.UsersetRewrite{
						RewriteOperation: &corev1.UsersetRewrite_Intersection{
							Intersection: &corev1.SetOperation{
								Child: []*corev1.SetOperation_Child{
									{
										ChildType: &corev1.SetOperation_Child_ComputedUserset{
											ComputedUserset: &corev1.ComputedUserset{
												Relation: "single_child",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		schema, err := BuildSchemaFromDefinitions([]*corev1.NamespaceDefinition{def}, nil)
		require.NoError(t, err)

		docDef, ok := schema.GetTypeDefinition("doc")
		require.True(t, ok)

		// Check union with single child (should be unwrapped)
		singleUnionPerm, ok := docDef.GetPermission("single_union")
		require.True(t, ok)

		singleUnionOp := singleUnionPerm.Operation()
		require.NotNil(t, singleUnionOp)
		require.NotNil(t, singleUnionOp.Parent())
		require.Equal(t, singleUnionPerm, singleUnionOp.Parent(), "Unwrapped operation should have permission as parent")

		// Check intersection with single child (should be unwrapped)
		singleIntersectionPerm, ok := docDef.GetPermission("single_intersection")
		require.True(t, ok)

		singleIntersectionOp := singleIntersectionPerm.Operation()
		require.NotNil(t, singleIntersectionOp)
		require.NotNil(t, singleIntersectionOp.Parent())
		require.Equal(t, singleIntersectionPerm, singleIntersectionOp.Parent(), "Unwrapped operation should have permission as parent")
	})
}
