package schema

import (
	"errors"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ToDefinitions converts a Schema to the full set of namespace and caveat definitions.
// This is useful for converting schemas back to the protobuf format for serialization.
func (s *Schema) ToDefinitions() ([]*core.NamespaceDefinition, []*core.CaveatDefinition, error) {
	if s == nil {
		return nil, nil, errors.New("cannot convert nil schema")
	}

	// Convert each definition
	definitions := make([]*core.NamespaceDefinition, 0, len(s.definitions))
	for defName, def := range s.definitions {
		relations, err := defToRelations(def)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to convert definition %s: %w", defName, err)
		}

		definitions = append(definitions, &core.NamespaceDefinition{
			Name:     defName,
			Relation: relations,
		})
	}

	// Convert caveats
	caveats := make([]*core.CaveatDefinition, 0, len(s.caveats))
	for caveatName, caveat := range s.caveats {
		caveats = append(caveats, &core.CaveatDefinition{
			Name:                 caveatName,
			SerializedExpression: []byte(caveat.expression),
			ParameterTypes:       make(map[string]*core.CaveatTypeReference), // TODO: populate if needed
		})
	}

	return definitions, caveats, nil
}

// defToRelations converts a Definition to a list of core.Relation.
func defToRelations(def *Definition) ([]*core.Relation, error) {
	relations := make([]*core.Relation, 0, len(def.relations)+len(def.permissions))

	// Convert relations
	for _, rel := range def.relations {
		relations = append(relations, &core.Relation{
			Name:            rel.name,
			TypeInformation: baseRelationsToTypeInfo(rel.baseRelations),
		})
	}

	// Convert permissions
	for _, perm := range def.permissions {
		rewrite, err := operationToUsersetRewrite(perm.operation)
		if err != nil {
			return nil, fmt.Errorf("failed to convert permission %s: %w", perm.name, err)
		}

		relations = append(relations, &core.Relation{
			Name:           perm.name,
			UsersetRewrite: rewrite,
		})
	}

	return relations, nil
}

// baseRelationsToTypeInfo converts BaseRelations to TypeInformation.
func baseRelationsToTypeInfo(baseRels []*BaseRelation) *core.TypeInformation {
	if len(baseRels) == 0 {
		return nil
	}

	allowedRels := make([]*core.AllowedRelation, len(baseRels))
	for i, br := range baseRels {
		allowedRels[i] = baseRelationToAllowedRelation(br)
	}

	return &core.TypeInformation{
		AllowedDirectRelations: allowedRels,
	}
}

// baseRelationToAllowedRelation converts a BaseRelation to an AllowedRelation.
func baseRelationToAllowedRelation(br *BaseRelation) *core.AllowedRelation {
	ar := &core.AllowedRelation{
		Namespace: br.subjectType,
	}

	if br.wildcard {
		ar.RelationOrWildcard = &core.AllowedRelation_PublicWildcard_{
			PublicWildcard: &core.AllowedRelation_PublicWildcard{},
		}
	} else {
		ar.RelationOrWildcard = &core.AllowedRelation_Relation{
			Relation: br.subrelation,
		}
	}

	if br.caveat != "" {
		ar.RequiredCaveat = &core.AllowedCaveat{
			CaveatName: br.caveat,
		}
	}

	if br.expiration {
		ar.RequiredExpiration = &core.ExpirationTrait{}
	}

	return ar
}

// operationToUsersetRewrite converts an Operation to a UsersetRewrite.
func operationToUsersetRewrite(op Operation) (*core.UsersetRewrite, error) {
	if op == nil {
		return nil, errors.New("nil operation")
	}

	switch o := op.(type) {
	case *ResolvedRelationReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_ComputedUserset{
								ComputedUserset: &core.ComputedUserset{
									Relation: o.relationName,
								},
							},
						},
					},
				},
			},
		}, nil

	case *RelationReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_ComputedUserset{
								ComputedUserset: &core.ComputedUserset{
									Relation: o.relationName,
								},
							},
						},
					},
				},
			},
		}, nil

	case *ResolvedArrowReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_TupleToUserset{
								TupleToUserset: &core.TupleToUserset{
									Tupleset: &core.TupleToUserset_Tupleset{
										Relation: o.left,
									},
									ComputedUserset: &core.ComputedUserset{
										Relation: o.right,
										Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
									},
								},
							},
						},
					},
				},
			},
		}, nil

	case *ResolvedFunctionedArrowReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_FunctionedTupleToUserset{
								FunctionedTupleToUserset: &core.FunctionedTupleToUserset{
									Function: functionTypeToCore(o.function),
									Tupleset: &core.FunctionedTupleToUserset_Tupleset{
										Relation: o.left,
									},
									ComputedUserset: &core.ComputedUserset{
										Relation: o.right,
										Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
									},
								},
							},
						},
					},
				},
			},
		}, nil

	case *FunctionedArrowReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_FunctionedTupleToUserset{
								FunctionedTupleToUserset: &core.FunctionedTupleToUserset{
									Function: functionTypeToCore(o.function),
									Tupleset: &core.FunctionedTupleToUserset_Tupleset{
										Relation: o.left,
									},
									ComputedUserset: &core.ComputedUserset{
										Relation: o.right,
										Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
									},
								},
							},
						},
					},
				},
			},
		}, nil

	case *ArrowReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_TupleToUserset{
								TupleToUserset: &core.TupleToUserset{
									Tupleset: &core.TupleToUserset_Tupleset{
										Relation: o.left,
									},
									ComputedUserset: &core.ComputedUserset{
										Relation: o.right,
										Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
									},
								},
							},
						},
					},
				},
			},
		}, nil

	case *UnionOperation:
		children, err := operationsToChildren(o.children)
		if err != nil {
			return nil, err
		}
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: children,
				},
			},
		}, nil

	case *IntersectionOperation:
		children, err := operationsToChildren(o.children)
		if err != nil {
			return nil, err
		}
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Intersection{
				Intersection: &core.SetOperation{
					Child: children,
				},
			},
		}, nil

	case *ExclusionOperation:
		leftChild, err := operationToChild(o.left)
		if err != nil {
			return nil, err
		}
		rightChild, err := operationToChild(o.right)
		if err != nil {
			return nil, err
		}
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Exclusion{
				Exclusion: &core.SetOperation{
					Child: []*core.SetOperation_Child{leftChild, rightChild},
				},
			},
		}, nil

	case *NilReference:
		return &core.UsersetRewrite{
			RewriteOperation: &core.UsersetRewrite_Union{
				Union: &core.SetOperation{
					Child: []*core.SetOperation_Child{
						{
							ChildType: &core.SetOperation_Child_XNil{},
						},
					},
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown operation type: %T", op)
	}
}

// operationsToChildren converts a slice of Operations to SetOperation_Child.
func operationsToChildren(ops []Operation) ([]*core.SetOperation_Child, error) {
	children := make([]*core.SetOperation_Child, len(ops))
	for i, op := range ops {
		child, err := operationToChild(op)
		if err != nil {
			return nil, err
		}
		children[i] = child
	}
	return children, nil
}

// operationToChild converts an Operation to a SetOperation_Child.
func operationToChild(op Operation) (*core.SetOperation_Child, error) {
	switch o := op.(type) {
	case *ResolvedRelationReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_ComputedUserset{
				ComputedUserset: &core.ComputedUserset{
					Relation: o.relationName,
				},
			},
		}, nil

	case *RelationReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_ComputedUserset{
				ComputedUserset: &core.ComputedUserset{
					Relation: o.relationName,
				},
			},
		}, nil

	case *ResolvedArrowReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_TupleToUserset{
				TupleToUserset: &core.TupleToUserset{
					Tupleset: &core.TupleToUserset_Tupleset{
						Relation: o.left,
					},
					ComputedUserset: &core.ComputedUserset{
						Relation: o.right,
						Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
					},
				},
			},
		}, nil

	case *ArrowReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_TupleToUserset{
				TupleToUserset: &core.TupleToUserset{
					Tupleset: &core.TupleToUserset_Tupleset{
						Relation: o.left,
					},
					ComputedUserset: &core.ComputedUserset{
						Relation: o.right,
						Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
					},
				},
			},
		}, nil

	case *ResolvedFunctionedArrowReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_FunctionedTupleToUserset{
				FunctionedTupleToUserset: &core.FunctionedTupleToUserset{
					Function: functionTypeToCore(o.function),
					Tupleset: &core.FunctionedTupleToUserset_Tupleset{
						Relation: o.left,
					},
					ComputedUserset: &core.ComputedUserset{
						Relation: o.right,
						Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
					},
				},
			},
		}, nil

	case *FunctionedArrowReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_FunctionedTupleToUserset{
				FunctionedTupleToUserset: &core.FunctionedTupleToUserset{
					Function: functionTypeToCore(o.function),
					Tupleset: &core.FunctionedTupleToUserset_Tupleset{
						Relation: o.left,
					},
					ComputedUserset: &core.ComputedUserset{
						Relation: o.right,
						Object:   core.ComputedUserset_TUPLE_USERSET_OBJECT,
					},
				},
			},
		}, nil

	case *UnionOperation, *IntersectionOperation, *ExclusionOperation:
		// Nested operations need to be wrapped in a UsersetRewrite
		rewrite, err := operationToUsersetRewrite(op)
		if err != nil {
			return nil, err
		}
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_UsersetRewrite{
				UsersetRewrite: rewrite,
			},
		}, nil

	case *NilReference:
		return &core.SetOperation_Child{
			ChildType: &core.SetOperation_Child_XNil{},
		}, nil

	default:
		return nil, fmt.Errorf("unknown operation type: %T", op)
	}
}

// functionTypeToCore converts a FunctionType to a core.FunctionedTupleToUserset_Function.
func functionTypeToCore(ft FunctionType) core.FunctionedTupleToUserset_Function {
	switch ft {
	case FunctionTypeAny:
		return core.FunctionedTupleToUserset_FUNCTION_ANY
	case FunctionTypeAll:
		return core.FunctionedTupleToUserset_FUNCTION_ALL
	default:
		return core.FunctionedTupleToUserset_FUNCTION_ANY
	}
}
