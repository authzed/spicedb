package namespace

import (
	"context"
	"fmt"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type AllowedDirectRelation int

const (
	// UnknownIfRelationAllowed indicates that no type information is defined for
	// this relation.
	UnknownIfRelationAllowed AllowedDirectRelation = iota

	// DirectRelationValid indicates that the specified subject relation is valid as
	// part of a *direct* tuple on the relation.
	DirectRelationValid

	// DirectRelationNotValid indicates that the specified subject relation is not
	// valid as part of a *direct* tuple on the relation.
	DirectRelationNotValid
)

// BuildNamespaceTypeSystem constructs a type system view of a namespace definition.
func BuildNamespaceTypeSystem(nsDef *pb.NamespaceDefinition, manager Manager, additionalDefinitions ...*pb.NamespaceDefinition) (*NamespaceTypeSystem, error) {
	relationMap := map[string]*pb.Relation{}
	for _, relation := range nsDef.GetRelation() {
		_, existing := relationMap[relation.Name]
		if existing {
			return nil, fmt.Errorf("Found duplicate relation name %s", relation.Name)
		}

		relationMap[relation.Name] = relation
	}

	return &NamespaceTypeSystem{
		manager:        manager,
		nsDef:          nsDef,
		relationMap:    relationMap,
		additionalDefs: additionalDefinitions,
	}, nil
}

// NamespaceTypeSystem represents typing information found in a namespace.
type NamespaceTypeSystem struct {
	manager        Manager
	nsDef          *pb.NamespaceDefinition
	relationMap    map[string]*pb.Relation
	additionalDefs []*pb.NamespaceDefinition
}

// HasRelation returns true if the namespace has the given relation defined.
func (nts *NamespaceTypeSystem) HasRelation(relationName string) bool {
	_, ok := nts.relationMap[relationName]
	return ok
}

// IsAllowedDirectRelation returns whether the subject relation is allowed to appear on the right
// hand side of a tuple placed in the source relation with the given name.
func (nts *NamespaceTypeSystem) IsAllowedDirectRelation(sourceRelationName string, targetNamespaceName string, targetRelationName string) (AllowedDirectRelation, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfRelationAllowed, fmt.Errorf("Unknown relation %s", sourceRelationName)
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return UnknownIfRelationAllowed, nil
	}

	allowedRelations := typeInfo.GetAllowedDirectRelations()
	for _, allowedRelation := range allowedRelations {
		if allowedRelation.GetNamespace() == targetNamespaceName && allowedRelation.GetRelation() == targetRelationName {
			return DirectRelationValid, nil
		}
	}

	return DirectRelationNotValid, nil
}

// AllowedDirectRelations returns the allowed subject relations for a source relation.
func (nts *NamespaceTypeSystem) AllowedDirectRelations(sourceRelationName string) ([]*pb.RelationReference, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return []*pb.RelationReference{}, fmt.Errorf("Unknown relation %s", sourceRelationName)
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return []*pb.RelationReference{}, nil
	}

	return typeInfo.GetAllowedDirectRelations(), nil
}

// Validate runs validation on the type system for the namespace to ensure it is consistent.
func (nts *NamespaceTypeSystem) Validate(ctx context.Context) error {
	for _, relation := range nts.relationMap {
		// Validate the usersets's.
		usersetRewrite := relation.GetUsersetRewrite()
		rerr := walkRewrite(usersetRewrite, func(childOneof *pb.SetOperation_Child) interface{} {
			switch child := childOneof.ChildType.(type) {
			case *pb.SetOperation_Child_ComputedUserset:
				relationName := child.ComputedUserset.GetRelation()
				_, ok := nts.relationMap[relationName]
				if !ok {
					return fmt.Errorf("In computed_userset for relation `%s`: relation `%s` not found", relation.Name, relationName)
				}
			case *pb.SetOperation_Child_TupleToUserset:
				ttu := child.TupleToUserset
				if ttu == nil {
					return nil
				}

				tupleset := ttu.GetTupleset()
				if tupleset == nil {
					return nil
				}

				relationName := tupleset.GetRelation()
				_, ok := nts.relationMap[relationName]
				if !ok {
					return fmt.Errorf("In tuple_to_userset for relation `%s`: relation `%s` not found", relation.Name, relationName)
				}
			}
			return nil
		})
		if rerr != nil {
			return rerr.(error)
		}

		// Validate type information.
		typeInfo := relation.TypeInformation
		if typeInfo == nil {
			continue
		}

		allowedDirectRelations := typeInfo.GetAllowedDirectRelations()

		// Check for a _this or the lack of a userset_rewrite. If either is found,
		// then the allowed list must have at least one type.
		hasThis := walkRewrite(usersetRewrite, func(childOneof *pb.SetOperation_Child) interface{} {
			switch childOneof.ChildType.(type) {
			case *pb.SetOperation_Child_XThis:
				return true
			}
			return nil
		})

		if usersetRewrite == nil || hasThis == true {
			if len(allowedDirectRelations) == 0 {
				return fmt.Errorf("At least one allowed relation is required in relation `%s`", relation.Name)
			}
		} else {
			if len(allowedDirectRelations) != 0 {
				return fmt.Errorf("No direct relations are allowed under relation `%s`", relation.Name)
			}
		}

		// Verify that all allowed relations are not this very relation, and that
		// they exist within the namespace.
		for _, allowedRelation := range allowedDirectRelations {
			if allowedRelation.GetNamespace() == nts.nsDef.Name {
				if allowedRelation.GetRelation() != "..." {
					_, ok := nts.relationMap[allowedRelation.GetRelation()]
					if !ok {
						return fmt.Errorf("For relation `%s`: relation `%s` not found under namespace `%s`", relation.Name, allowedRelation.GetRelation(), allowedRelation.GetNamespace())
					}
				}
			} else {
				subjectTS, err := nts.typeSystemForNamespace(ctx, allowedRelation.GetNamespace())
				if err != nil {
					return fmt.Errorf("Could not lookup namespace `%s` for relation `%s`: %w", allowedRelation.GetNamespace(), relation.Name, err)
				}

				if allowedRelation.GetRelation() != "..." {
					ok := subjectTS.HasRelation(allowedRelation.GetRelation())
					if !ok {
						return fmt.Errorf("For relation `%s`: relation `%s` not found under namespace `%s`", relation.Name, allowedRelation.GetRelation(), allowedRelation.GetNamespace())
					}
				}
			}
		}
	}

	return nil
}

func (nts *NamespaceTypeSystem) typeSystemForNamespace(ctx context.Context, namespaceName string) (*NamespaceTypeSystem, error) {
	if nts.nsDef.Name == namespaceName {
		return nts, nil
	}

	// NOTE: Order is important here: We always check the new definitions before the existing
	// ones.

	// Check the additional namespace definitions for the namespace.
	for _, additionalDef := range nts.additionalDefs {
		if additionalDef.Name == namespaceName {
			return BuildNamespaceTypeSystem(additionalDef, nts.manager)
		}
	}

	// Otherwise, check already defined namespaces.
	otherNamespaceDef, _, err := nts.manager.ReadNamespace(ctx, namespaceName)
	if err != nil {
		return nil, err
	}

	return BuildNamespaceTypeSystem(otherNamespaceDef, nts.manager)
}

type walkHandler func(childOneof *pb.SetOperation_Child) interface{}

func walkRewrite(rewrite *pb.UsersetRewrite, handler walkHandler) interface{} {
	if rewrite == nil {
		return nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		return walkRewriteChildren(rw.Union, handler)
	case *pb.UsersetRewrite_Intersection:
		return walkRewriteChildren(rw.Intersection, handler)
	case *pb.UsersetRewrite_Exclusion:
		return walkRewriteChildren(rw.Exclusion, handler)
	}
	return nil
}

func walkRewriteChildren(so *pb.SetOperation, handler walkHandler) interface{} {
	for _, childOneof := range so.Child {
		vle := handler(childOneof)
		if vle != nil {
			return vle
		}

		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_UsersetRewrite:
			rvle := walkRewrite(child.UsersetRewrite, handler)
			if rvle != nil {
				return rvle
			}
		}
	}

	return nil
}
