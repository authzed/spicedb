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
