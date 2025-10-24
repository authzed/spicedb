package schema

import (
	"errors"
	"fmt"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func convertDefinition(def *corev1.NamespaceDefinition) (*Definition, error) {
	out := &Definition{
		name:        def.GetName(),
		relations:   make(map[string]*Relation),
		permissions: make(map[string]*Permission),
	}
	for _, r := range def.GetRelation() {
		if userset := r.GetUsersetRewrite(); userset != nil {
			perm, err := convertUserset(userset)
			if err != nil {
				return nil, err
			}
			perm.parent = out
			perm.name = r.GetName()
			out.permissions[r.GetName()] = &perm
		} else if typeinfo := r.GetTypeInformation(); typeinfo != nil {
			rel, err := convertTypeInformation(typeinfo)
			if err != nil {
				return nil, err
			}
			rel.parent = out
			rel.name = r.GetName()
			out.relations[r.GetName()] = rel
		}
	}
	return out, nil
}

func convertCaveat(def *corev1.CaveatDefinition) (*Caveat, error) {
	out := &Caveat{
		name:       def.GetName(),
		expression: string(def.GetSerializedExpression()),
		parameters: make([]CaveatParameter, 0, len(def.GetParameterTypes())),
	}

	for paramName, paramType := range def.GetParameterTypes() {
		out.parameters = append(out.parameters, CaveatParameter{
			name: paramName,
			typ:  paramType.GetTypeName(),
		})
	}

	return out, nil
}

func convertUserset(userset *corev1.UsersetRewrite) (Permission, error) {
	if userset == nil {
		return Permission{}, errors.New("userset rewrite is nil")
	}

	var operation Operation
	var err error

	switch rewrite := userset.GetRewriteOperation().(type) {
	case *corev1.UsersetRewrite_Union:
		operation, err = convertSetOperation(rewrite.Union)
		if err != nil {
			return Permission{}, err
		}
	case *corev1.UsersetRewrite_Intersection:
		operation, err = convertSetOperationAsIntersection(rewrite.Intersection)
		if err != nil {
			return Permission{}, err
		}
	case *corev1.UsersetRewrite_Exclusion:
		operation, err = convertSetOperationAsExclusion(rewrite.Exclusion)
		if err != nil {
			return Permission{}, err
		}
	default:
		return Permission{}, errors.New("unknown userset rewrite operation type")
	}

	return Permission{
		operation: operation,
	}, nil
}

func convertTypeInformation(typeinfo *corev1.TypeInformation) (*Relation, error) {
	if typeinfo == nil {
		return &Relation{}, errors.New("type information is nil")
	}

	thisRelation := &Relation{}

	thisRelation.baseRelations = make([]*BaseRelation, 0, len(typeinfo.GetAllowedDirectRelations()))
	for _, allowedRelation := range typeinfo.GetAllowedDirectRelations() {
		// Extract caveat and expiration information
		caveat := ""
		if allowedRelation.GetRequiredCaveat() != nil {
			caveat = allowedRelation.GetRequiredCaveat().GetCaveatName()
		}
		expiration := allowedRelation.GetRequiredExpiration() != nil

		if allowedRelation.GetPublicWildcard() != nil {
			thisRelation.baseRelations = append(thisRelation.baseRelations, &BaseRelation{
				parent:      thisRelation,
				subjectType: allowedRelation.GetNamespace(),
				wildcard:    true,
				caveat:      caveat,
				expiration:  expiration,
			})
		} else {
			relationName := allowedRelation.GetRelation()
			if relationName == "" || relationName == "..." {
				thisRelation.baseRelations = append(thisRelation.baseRelations, &BaseRelation{
					parent:      thisRelation,
					subjectType: allowedRelation.GetNamespace(),
					subrelation: tuple.Ellipsis,
					caveat:      caveat,
					expiration:  expiration,
				})
			} else {
				thisRelation.baseRelations = append(thisRelation.baseRelations, &BaseRelation{
					parent:      thisRelation,
					subjectType: allowedRelation.GetNamespace(),
					subrelation: relationName,
					caveat:      caveat,
					expiration:  expiration,
				})
			}
		}
	}

	return thisRelation, nil
}

func convertSetOperation(setOp *corev1.SetOperation) (Operation, error) {
	if setOp == nil {
		return nil, errors.New("set operation is nil")
	}

	children := make([]Operation, 0, len(setOp.GetChild()))
	for _, child := range setOp.GetChild() {
		childOp, err := convertChild(child)
		if err != nil {
			return nil, err
		}
		children = append(children, childOp)
	}

	// Optimize: if there's only one child, return it directly instead of wrapping in UnionOperation
	if len(children) == 1 {
		return children[0], nil
	}

	return &UnionOperation{
		children: children,
	}, nil
}

func convertSetOperationAsIntersection(setOp *corev1.SetOperation) (Operation, error) {
	if setOp == nil {
		return nil, errors.New("set operation is nil")
	}

	children := make([]Operation, 0, len(setOp.GetChild()))
	for _, child := range setOp.GetChild() {
		childOp, err := convertChild(child)
		if err != nil {
			return nil, err
		}
		children = append(children, childOp)
	}

	// Optimize: if there's only one child, return it directly instead of wrapping in IntersectionOperation
	if len(children) == 1 {
		return children[0], nil
	}

	return &IntersectionOperation{
		children: children,
	}, nil
}

func convertSetOperationAsExclusion(setOp *corev1.SetOperation) (Operation, error) {
	if setOp == nil {
		return nil, errors.New("set operation is nil")
	}

	children := make([]Operation, 0, len(setOp.GetChild()))
	for _, child := range setOp.GetChild() {
		childOp, err := convertChild(child)
		if err != nil {
			return nil, err
		}
		children = append(children, childOp)
	}

	if len(children) != 2 {
		return nil, fmt.Errorf("exclusion operation requires exactly 2 children, got %d", len(children))
	}

	return &ExclusionOperation{
		left:  children[0],
		right: children[1],
	}, nil
}

func convertChild(child *corev1.SetOperation_Child) (Operation, error) {
	if child == nil {
		return nil, errors.New("child is nil")
	}

	switch childType := child.GetChildType().(type) {
	case *corev1.SetOperation_Child_XThis:
		return &RelationReference{
			relationName: "_this",
		}, nil
	case *corev1.SetOperation_Child_ComputedUserset:
		return &RelationReference{
			relationName: childType.ComputedUserset.GetRelation(),
		}, nil
	case *corev1.SetOperation_Child_UsersetRewrite:
		perm, err := convertUserset(childType.UsersetRewrite)
		if err != nil {
			return nil, err
		}
		return perm.operation, nil
	case *corev1.SetOperation_Child_TupleToUserset:
		return &ArrowReference{
			left:  childType.TupleToUserset.GetTupleset().GetRelation(),
			right: childType.TupleToUserset.GetComputedUserset().GetRelation(),
		}, nil
	case *corev1.SetOperation_Child_FunctionedTupleToUserset:
		functionType, err := convertFunctionType(childType.FunctionedTupleToUserset.GetFunction())
		if err != nil {
			return nil, err
		}
		return &FunctionedArrowReference{
			left:     childType.FunctionedTupleToUserset.GetTupleset().GetRelation(),
			right:    childType.FunctionedTupleToUserset.GetComputedUserset().GetRelation(),
			function: functionType,
		}, nil
	case *corev1.SetOperation_Child_XNil:
		return &NilReference{}, nil
	default:
		return nil, fmt.Errorf("unknown child type: %#v", child.GetChildType())
	}
}

func convertFunctionType(protoFunc corev1.FunctionedTupleToUserset_Function) (FunctionType, error) {
	switch protoFunc {
	case corev1.FunctionedTupleToUserset_FUNCTION_ANY:
		return FunctionTypeAny, nil
	case corev1.FunctionedTupleToUserset_FUNCTION_ALL:
		return FunctionTypeAll, nil
	default:
		return 0, fmt.Errorf("unknown function type: %v", protoFunc)
	}
}
