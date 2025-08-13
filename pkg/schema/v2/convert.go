package schema

import (
	"fmt"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func convertDefinition(def *corev1.NamespaceDefinition) (*Definition, error) {
	out := &Definition{
		Name:        def.GetName(),
		Relations:   make(map[string]*Relation),
		Permissions: make(map[string]*Permission),
	}
	for _, r := range def.GetRelation() {
		if userset := r.GetUsersetRewrite(); userset != nil {
			perm, err := convertUserset(userset)
			if err != nil {
				return nil, err
			}
			perm.Parent = out
			perm.Name = r.GetName()
			out.Permissions[r.GetName()] = &perm
		} else if typeinfo := r.GetTypeInformation(); typeinfo != nil {
			rel, err := convertTypeInformation(typeinfo)
			if err != nil {
				return nil, err
			}
			rel.Parent = out
			rel.Name = r.GetName()
			out.Relations[r.GetName()] = rel
		}
	}
	return out, nil
}

func convertCaveat(def *corev1.CaveatDefinition) (*Caveat, error) {
	out := &Caveat{
		Name:           def.GetName(),
		Expression:     string(def.GetSerializedExpression()),
		ParameterTypes: make([]string, 0, len(def.GetParameterTypes())),
	}

	for paramName := range def.GetParameterTypes() {
		out.ParameterTypes = append(out.ParameterTypes, paramName)
	}

	return out, nil
}

func convertUserset(userset *corev1.UsersetRewrite) (Permission, error) {
	if userset == nil {
		return Permission{}, fmt.Errorf("userset rewrite is nil")
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
		return Permission{}, fmt.Errorf("unknown userset rewrite operation type")
	}

	return Permission{
		Operation: operation,
	}, nil
}

func convertTypeInformation(typeinfo *corev1.TypeInformation) (*Relation, error) {
	if typeinfo == nil {
		return &Relation{}, fmt.Errorf("type information is nil")
	}

	thisRelation := &Relation{}

	thisRelation.BaseRelations = make([]*BaseRelation, 0, len(typeinfo.GetAllowedDirectRelations()))
	for _, allowedRelation := range typeinfo.GetAllowedDirectRelations() {
		// Extract caveat and expiration information
		caveat := ""
		if allowedRelation.GetRequiredCaveat() != nil {
			caveat = allowedRelation.GetRequiredCaveat().GetCaveatName()
		}
		expiration := allowedRelation.GetRequiredExpiration() != nil

		if allowedRelation.GetPublicWildcard() != nil {
			thisRelation.BaseRelations = append(thisRelation.BaseRelations, &BaseRelation{
				Parent:     thisRelation,
				Type:       allowedRelation.GetNamespace(),
				Wildcard:   true,
				Caveat:     caveat,
				Expiration: expiration,
			})
		} else {
			relationName := allowedRelation.GetRelation()
			if relationName == "" || relationName == "..." {
				thisRelation.BaseRelations = append(thisRelation.BaseRelations, &BaseRelation{
					Parent:     thisRelation,
					Type:       allowedRelation.GetNamespace(),
					Caveat:     caveat,
					Expiration: expiration,
				})
			} else {
				thisRelation.BaseRelations = append(thisRelation.BaseRelations, &BaseRelation{
					Parent:      thisRelation,
					Type:        allowedRelation.GetNamespace(),
					Subrelation: relationName,
					Caveat:      caveat,
					Expiration:  expiration,
				})
			}
		}
	}

	return thisRelation, nil
}

func convertSetOperation(setOp *corev1.SetOperation) (Operation, error) {
	if setOp == nil {
		return nil, fmt.Errorf("set operation is nil")
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
		Children: children,
	}, nil
}

func convertSetOperationAsIntersection(setOp *corev1.SetOperation) (Operation, error) {
	if setOp == nil {
		return nil, fmt.Errorf("set operation is nil")
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
		Children: children,
	}, nil
}

func convertSetOperationAsExclusion(setOp *corev1.SetOperation) (Operation, error) {
	if setOp == nil {
		return nil, fmt.Errorf("set operation is nil")
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
		Left:  children[0],
		Right: children[1],
	}, nil
}

func convertChild(child *corev1.SetOperation_Child) (Operation, error) {
	if child == nil {
		return nil, fmt.Errorf("child is nil")
	}

	switch childType := child.GetChildType().(type) {
	case *corev1.SetOperation_Child_XThis:
		return &RelationReference{
			RelationName: "_this",
		}, nil
	case *corev1.SetOperation_Child_ComputedUserset:
		return &RelationReference{
			RelationName: childType.ComputedUserset.GetRelation(),
		}, nil
	case *corev1.SetOperation_Child_UsersetRewrite:
		perm, err := convertUserset(childType.UsersetRewrite)
		if err != nil {
			return nil, err
		}
		return perm.Operation, nil
	case *corev1.SetOperation_Child_TupleToUserset:
		return &RelationReference{
			RelationName: childType.TupleToUserset.GetTupleset().GetRelation() + "->" + childType.TupleToUserset.GetComputedUserset().GetRelation(),
		}, nil
	case *corev1.SetOperation_Child_XNil:
		return &RelationReference{
			RelationName: "_nil",
		}, nil
	default:
		return nil, fmt.Errorf("unknown child type")
	}
}
