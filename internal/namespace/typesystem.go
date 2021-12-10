package namespace

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"

	iv1 "github.com/authzed/spicedb/internal/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/graph"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
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

// LookupNamespace is a function used to lookup a namespace.
type LookupNamespace func(ctx context.Context, name string) (*v0.NamespaceDefinition, error)

// BuildNamespaceTypeSystemWithFallback constructs a type system view of a namespace definition, with automatic lookup
// via the additional defs first, and then the namespace manager as a fallback.
func BuildNamespaceTypeSystemWithFallback(nsDef *v0.NamespaceDefinition, manager Manager, additionalDefs []*v0.NamespaceDefinition, revision decimal.Decimal) (*NamespaceTypeSystem, error) {
	return BuildNamespaceTypeSystem(nsDef, func(ctx context.Context, namespaceName string) (*v0.NamespaceDefinition, error) {
		// NOTE: Order is important here: We always check the new definitions before the existing
		// ones.

		// Check the additional namespace definitions for the namespace.
		for _, additionalDef := range additionalDefs {
			if additionalDef.Name == namespaceName {
				return additionalDef, nil
			}
		}

		// Otherwise, check already defined namespaces.
		otherNamespaceDef, err := manager.ReadNamespace(ctx, namespaceName, revision)
		return otherNamespaceDef, err
	})
}

// BuildNamespaceTypeSystemForManager constructs a type system view of a namespace definition, with automatic lookup
// via the namespace manager.
func BuildNamespaceTypeSystemForManager(nsDef *v0.NamespaceDefinition, manager Manager, revision decimal.Decimal) (*NamespaceTypeSystem, error) {
	return BuildNamespaceTypeSystem(nsDef, func(ctx context.Context, nsName string) (*v0.NamespaceDefinition, error) {
		nsDef, err := manager.ReadNamespace(ctx, nsName, revision)
		return nsDef, err
	})
}

// BuildNamespaceTypeSystemForDefs constructs a type system view of a namespace definition, with lookup in the
// list of definitions given.
func BuildNamespaceTypeSystemForDefs(nsDef *v0.NamespaceDefinition, allDefs []*v0.NamespaceDefinition) (*NamespaceTypeSystem, error) {
	return BuildNamespaceTypeSystem(nsDef, func(ctx context.Context, nsName string) (*v0.NamespaceDefinition, error) {
		for _, def := range allDefs {
			if def.Name == nsName {
				return def, nil
			}
		}

		return nil, fmt.Errorf("unknown definition %s", nsName)
	})
}

// BuildNamespaceTypeSystem constructs a type system view of a namespace definition.
func BuildNamespaceTypeSystem(nsDef *v0.NamespaceDefinition, lookupNamespace LookupNamespace) (*NamespaceTypeSystem, error) {
	relationMap := map[string]*v0.Relation{}
	for _, relation := range nsDef.GetRelation() {
		_, existing := relationMap[relation.Name]
		if existing {
			return nil, fmt.Errorf("found duplicate relation/permission name %s", relation.Name)
		}

		relationMap[relation.Name] = relation
	}

	return &NamespaceTypeSystem{
		lookupNamespace: lookupNamespace,
		nsDef:           nsDef,
		relationMap:     relationMap,
	}, nil
}

// NamespaceTypeSystem represents typing information found in a namespace.
type NamespaceTypeSystem struct {
	lookupNamespace LookupNamespace
	nsDef           *v0.NamespaceDefinition
	relationMap     map[string]*v0.Relation
}

// HasTypeInformation returns true if the relation with the given name exists and has type
// information defined.
func (nts *NamespaceTypeSystem) HasTypeInformation(relationName string) bool {
	rel, ok := nts.relationMap[relationName]
	return ok && rel.GetTypeInformation() != nil
}

// HasRelation returns true if the namespace has the given relation defined.
func (nts *NamespaceTypeSystem) HasRelation(relationName string) bool {
	_, ok := nts.relationMap[relationName]
	return ok
}

// IsPermission returns true if the namespace has the given relation defined and it is
// a permission.
func (nts *NamespaceTypeSystem) IsPermission(relationName string) bool {
	found, ok := nts.relationMap[relationName]
	if !ok {
		return false
	}

	return nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION
}

// IsAllowedDirectRelation returns whether the subject relation is allowed to appear on the right
// hand side of a tuple placed in the source relation with the given name.
func (nts *NamespaceTypeSystem) IsAllowedDirectRelation(sourceRelationName string, targetNamespaceName string, targetRelationName string) (AllowedDirectRelation, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfRelationAllowed, fmt.Errorf("unknown relation/permission `%s` under permissions system `%s`", sourceRelationName, nts.nsDef.Name)
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
func (nts *NamespaceTypeSystem) AllowedDirectRelations(sourceRelationName string) ([]*v0.RelationReference, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return []*v0.RelationReference{}, fmt.Errorf("unknown relation/permission `%s` under permissions system `%s`", sourceRelationName, nts.nsDef.Name)
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return []*v0.RelationReference{}, nil
	}

	return typeInfo.GetAllowedDirectRelations(), nil
}

// Validate runs validation on the type system for the namespace to ensure it is consistent.
func (nts *NamespaceTypeSystem) Validate(ctx context.Context) error {
	for _, relation := range nts.relationMap {
		// Validate the usersets's.
		usersetRewrite := relation.GetUsersetRewrite()
		rerr := graph.WalkRewrite(usersetRewrite, func(childOneof *v0.SetOperation_Child) interface{} {
			switch child := childOneof.ChildType.(type) {
			case *v0.SetOperation_Child_ComputedUserset:
				relationName := child.ComputedUserset.GetRelation()
				_, ok := nts.relationMap[relationName]
				if !ok {
					return fmt.Errorf("under permission `%s`: relation/permission `%s` was not found", relation.Name, relationName)
				}
			case *v0.SetOperation_Child_TupleToUserset:
				ttu := child.TupleToUserset
				if ttu == nil {
					return nil
				}

				tupleset := ttu.GetTupleset()
				if tupleset == nil {
					return nil
				}

				relationName := tupleset.GetRelation()
				found, ok := nts.relationMap[relationName]
				if !ok {
					return fmt.Errorf("under permission `%s`: relation `%s` was not found", relation.Name, relationName)
				}

				if nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION {
					return fmt.Errorf("under permission `%s`: permissions cannot be used on the left hand side of an arrow (found `%s`)", relation.Name, relationName)
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
		hasThis := graph.HasThis(usersetRewrite)

		if usersetRewrite == nil || hasThis {
			if len(allowedDirectRelations) == 0 {
				return fmt.Errorf("at least one allowed relation/permission is required in relation `%s`", relation.Name)
			}
		} else {
			if len(allowedDirectRelations) != 0 {
				return fmt.Errorf("direct relations are not allowed under relation `%s`", relation.Name)
			}
		}

		// Verify that all allowed relations are not this very relation, and that
		// they exist within the namespace.
		for _, allowedRelation := range allowedDirectRelations {
			if allowedRelation.GetNamespace() == nts.nsDef.Name {
				if allowedRelation.GetRelation() != "..." {
					_, ok := nts.relationMap[allowedRelation.GetRelation()]
					if !ok {
						return fmt.Errorf("for relation `%s`: relation/permission `%s` was not found under definition `%s`", relation.Name, allowedRelation.GetRelation(), allowedRelation.GetNamespace())
					}
				}
			} else {
				subjectTS, err := nts.typeSystemForNamespace(ctx, allowedRelation.GetNamespace())
				if err != nil {
					return fmt.Errorf("could not lookup definition `%s` for relation `%s`: %w", allowedRelation.GetNamespace(), relation.Name, err)
				}

				if allowedRelation.GetRelation() != "..." {
					ok := subjectTS.HasRelation(allowedRelation.GetRelation())
					if !ok {
						return fmt.Errorf("for relation `%s`: relation/permission `%s` was not found under definition `%s`", relation.Name, allowedRelation.GetRelation(), allowedRelation.GetNamespace())
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

	nsDef, err := nts.lookupNamespace(ctx, namespaceName)
	if err != nil {
		return nil, err
	}

	return BuildNamespaceTypeSystem(nsDef, nts.lookupNamespace)
}
