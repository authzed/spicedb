package namespace

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/commonerrors"
	"github.com/authzed/spicedb/pkg/graph"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// AllowedDirectRelation indicates whether a relation is allowed on the right side of another relation.
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

// AllowedPublicSubject indicates whether a public subject of a particular kind is allowed on the right side of another relation.
type AllowedPublicSubject int

const (
	// UnknownIfPublicAllowed indicates that no type information is defined for
	// this relation.
	UnknownIfPublicAllowed AllowedPublicSubject = iota

	// PublicSubjectAllowed indicates that the specified subject wildcard is valid as
	// part of a *direct* tuple on the relation.
	PublicSubjectAllowed

	// PublicSubjectNotAllowed indicates that the specified subject wildcard is not
	// valid as part of a *direct* tuple on the relation.
	PublicSubjectNotAllowed
)

// LookupNamespace is a function used to lookup a namespace.
type LookupNamespace func(ctx context.Context, name string) (*core.NamespaceDefinition, error)

// BuildNamespaceTypeSystemWithFallback constructs a type system view of a namespace definition, with automatic lookup
// via the additional defs first, and then the namespace manager as a fallback.
func BuildNamespaceTypeSystemWithFallback(nsDef *core.NamespaceDefinition, ds datastore.Reader, additionalDefs []*core.NamespaceDefinition) (*TypeSystem, error) {
	return BuildNamespaceTypeSystem(nsDef, func(ctx context.Context, namespaceName string) (*core.NamespaceDefinition, error) {
		// NOTE: Order is important here: We always check the new definitions before the existing
		// ones.

		// Check the additional namespace definitions for the namespace.
		for _, additionalDef := range additionalDefs {
			if additionalDef.Name == namespaceName {
				return additionalDef, nil
			}
		}

		// Otherwise, check already defined namespaces.
		otherNamespaceDef, _, err := ds.ReadNamespace(ctx, namespaceName)
		return otherNamespaceDef, err
	})
}

// BuildNamespaceTypeSystemForDatastore constructs a type system view of a namespace definition, with automatic lookup
// via the datastore reader.
func BuildNamespaceTypeSystemForDatastore(nsDef *core.NamespaceDefinition, ds datastore.Reader) (*TypeSystem, error) {
	return BuildNamespaceTypeSystem(nsDef, func(ctx context.Context, nsName string) (*core.NamespaceDefinition, error) {
		nsDef, _, err := ds.ReadNamespace(ctx, nsName)
		return nsDef, err
	})
}

// BuildNamespaceTypeSystemForDefs constructs a type system view of a namespace definition, with lookup in the
// list of definitions given.
func BuildNamespaceTypeSystemForDefs(nsDef *core.NamespaceDefinition, allDefs []*core.NamespaceDefinition) (*TypeSystem, error) {
	return BuildNamespaceTypeSystem(nsDef, func(ctx context.Context, nsName string) (*core.NamespaceDefinition, error) {
		for _, def := range allDefs {
			if def.Name == nsName {
				return def, nil
			}
		}

		return nil, fmt.Errorf("unknown definition %s", nsName)
	})
}

func newErrorWithSource(withSource nspkg.WithSourcePosition, sourceCodeString string, message string, args ...interface{}) error {
	sourcePosition := withSource.GetSourcePosition()
	if sourcePosition != nil {
		return commonerrors.NewErrorWithSource(
			fmt.Errorf(message, args...),
			sourceCodeString,
			sourcePosition.ZeroIndexedLineNumber+1,     // +1 to make 1-indexed
			sourcePosition.ZeroIndexedColumnPosition+1, // +1 to make 1-indexed
		)
	}

	return commonerrors.NewErrorWithSource(
		fmt.Errorf(message, args...),
		sourceCodeString,
		0,
		0,
	)
}

// BuildNamespaceTypeSystem constructs a type system view of a namespace definition.
func BuildNamespaceTypeSystem(nsDef *core.NamespaceDefinition, lookupNamespace LookupNamespace) (*TypeSystem, error) {
	relationMap := map[string]*core.Relation{}
	for _, relation := range nsDef.GetRelation() {
		_, existing := relationMap[relation.Name]
		if existing {
			return nil, newErrorWithSource(relation, relation.Name, "found duplicate relation/permission name `%s`", relation.Name)
		}

		relationMap[relation.Name] = relation
	}

	return &TypeSystem{
		lookupNamespace:    lookupNamespace,
		nsDef:              nsDef,
		relationMap:        relationMap,
		wildcardCheckCache: map[string]*WildcardTypeReference{},
	}, nil
}

// TypeSystem represents typing information found in a namespace.
type TypeSystem struct {
	lookupNamespace    LookupNamespace
	nsDef              *core.NamespaceDefinition
	relationMap        map[string]*core.Relation
	wildcardCheckCache map[string]*WildcardTypeReference
}

// Namespace is the namespace for which the type system was constructed.
func (nts *TypeSystem) Namespace() *core.NamespaceDefinition {
	return nts.nsDef
}

// HasTypeInformation returns true if the relation with the given name exists and has type
// information defined.
func (nts *TypeSystem) HasTypeInformation(relationName string) bool {
	rel, ok := nts.relationMap[relationName]
	return ok && rel.GetTypeInformation() != nil
}

// HasRelation returns true if the namespace has the given relation defined.
func (nts *TypeSystem) HasRelation(relationName string) bool {
	_, ok := nts.relationMap[relationName]
	return ok
}

// IsPermission returns true if the namespace has the given relation defined and it is
// a permission.
func (nts *TypeSystem) IsPermission(relationName string) bool {
	found, ok := nts.relationMap[relationName]
	if !ok {
		return false
	}

	return nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION
}

// IsAllowedPublicNamespace returns whether the target namespace is defined as public on the source relation.
func (nts *TypeSystem) IsAllowedPublicNamespace(sourceRelationName string, targetNamespaceName string) (AllowedPublicSubject, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfPublicAllowed, fmt.Errorf("unknown relation/permission `%s` under permissions system `%s`", sourceRelationName, nts.nsDef.Name)
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return UnknownIfPublicAllowed, nil
	}

	allowedRelations := typeInfo.GetAllowedDirectRelations()
	for _, allowedRelation := range allowedRelations {
		if allowedRelation.GetNamespace() == targetNamespaceName && allowedRelation.GetPublicWildcard() != nil {
			return PublicSubjectAllowed, nil
		}
	}

	return PublicSubjectNotAllowed, nil
}

// IsAllowedDirectRelation returns whether the subject relation is allowed to appear on the right
// hand side of a tuple placed in the source relation with the given name.
func (nts *TypeSystem) IsAllowedDirectRelation(sourceRelationName string, targetNamespaceName string, targetRelationName string) (AllowedDirectRelation, error) {
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

// AllowedDirectRelationsAndWildcards returns the allowed subject relations for a source relation. Note that this function will return
// wildcards.
func (nts *TypeSystem) AllowedDirectRelationsAndWildcards(sourceRelationName string) ([]*core.AllowedRelation, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return []*core.AllowedRelation{}, fmt.Errorf("unknown relation/permission `%s` under permissions system `%s`", sourceRelationName, nts.nsDef.Name)
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return []*core.AllowedRelation{}, nil
	}

	return typeInfo.GetAllowedDirectRelations(), nil
}

// AllowedSubjectRelations returns the allowed subject relations for a source relation. Note that this function will *not*
// return wildcards.
func (nts *TypeSystem) AllowedSubjectRelations(sourceRelationName string) ([]*core.RelationReference, error) {
	allowedDirect, err := nts.AllowedDirectRelationsAndWildcards(sourceRelationName)
	if err != nil {
		return []*core.RelationReference{}, err
	}

	filtered := make([]*core.RelationReference, 0, len(allowedDirect))
	for _, allowed := range allowedDirect {
		if allowed.GetPublicWildcard() != nil {
			continue
		}

		if allowed.GetRelation() == "" {
			panic("Got an empty relation for a non-wildcard type definition under namespace")
		}

		filtered = append(filtered, &core.RelationReference{
			Namespace: allowed.GetNamespace(),
			Relation:  allowed.GetRelation(),
		})
	}
	return filtered, nil
}

// WildcardTypeReference represents a relation that references a wildcard type.
type WildcardTypeReference struct {
	// ReferencingRelation is the relation referencing the wildcard type.
	ReferencingRelation *core.RelationReference

	// WildcardType is the wildcard type referenced.
	WildcardType *core.AllowedRelation
}

// ReferencesWildcardType returns true if the relation references a wildcard type, either directly or via
// another relation.
func (nts *TypeSystem) ReferencesWildcardType(ctx context.Context, relationName string) (*WildcardTypeReference, error) {
	return nts.referencesWildcardType(ctx, relationName, map[string]bool{})
}

func (nts *TypeSystem) referencesWildcardType(ctx context.Context, relationName string, encountered map[string]bool) (*WildcardTypeReference, error) {
	cached, isCached := nts.wildcardCheckCache[relationName]
	if isCached {
		return cached, nil
	}

	computed, err := nts.computeReferencesWildcardType(ctx, relationName, encountered)
	if err != nil {
		return nil, err
	}

	nts.wildcardCheckCache[relationName] = computed
	return computed, nil
}

func (nts *TypeSystem) computeReferencesWildcardType(ctx context.Context, relationName string, encountered map[string]bool) (*WildcardTypeReference, error) {
	relString := fmt.Sprintf("%s#%s", nts.nsDef.Name, relationName)
	if _, ok := encountered[relString]; ok {
		return nil, nil
	}
	encountered[relString] = true

	allowedRels, err := nts.AllowedDirectRelationsAndWildcards(relationName)
	if err != nil {
		return nil, err
	}

	for _, allowedRelation := range allowedRels {
		if allowedRelation.GetPublicWildcard() != nil {
			return &WildcardTypeReference{
				ReferencingRelation: &core.RelationReference{
					Namespace: nts.nsDef.Name,
					Relation:  relationName,
				},
				WildcardType: allowedRelation,
			}, nil
		}

		if allowedRelation.GetRelation() != tuple.Ellipsis {
			if allowedRelation.GetNamespace() == nts.nsDef.Name {
				found, err := nts.referencesWildcardType(ctx, allowedRelation.GetRelation(), encountered)
				if err != nil {
					return nil, err
				}

				if found != nil {
					return found, nil
				}
				continue
			}

			subjectTS, err := nts.typeSystemForNamespace(ctx, allowedRelation.GetNamespace())
			if err != nil {
				return nil, err
			}

			found, err := subjectTS.referencesWildcardType(ctx, allowedRelation.GetRelation(), encountered)
			if err != nil {
				return nil, err
			}

			if found != nil {
				return found, nil
			}
		}
	}

	return nil, nil
}

// AsValidated returns the current type system marked as validated. This method should *only* be
// called for type systems read from storage.
// TODO(jschorr): Maybe have the namespaces loaded from datastore do this automatically?
func (nts *TypeSystem) AsValidated() *ValidatedNamespaceTypeSystem {
	return &ValidatedNamespaceTypeSystem{nts}
}

// Validate runs validation on the type system for the namespace to ensure it is consistent.
func (nts *TypeSystem) Validate(ctx context.Context) (*ValidatedNamespaceTypeSystem, error) {
	for _, relation := range nts.relationMap {
		// Validate the usersets's.
		usersetRewrite := relation.GetUsersetRewrite()
		rerr := graph.WalkRewrite(usersetRewrite, func(childOneof *core.SetOperation_Child) interface{} {
			switch child := childOneof.ChildType.(type) {
			case *core.SetOperation_Child_ComputedUserset:
				relationName := child.ComputedUserset.GetRelation()
				_, ok := nts.relationMap[relationName]
				if !ok {
					return newErrorWithSource(childOneof, relationName, "under permission `%s`: relation/permission `%s` was not found", relation.Name, relationName)
				}
			case *core.SetOperation_Child_TupleToUserset:
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
					return newErrorWithSource(childOneof, relationName, "under permission `%s`: relation `%s` was not found", relation.Name, relationName)
				}

				if nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION {
					return newErrorWithSource(childOneof, relationName, "under permission `%s`: permissions cannot be used on the left hand side of an arrow (found `%s`)", relation.Name, relationName)
				}

				// Ensure the tupleset relation doesn't itself import wildcard.
				referencedWildcard, err := nts.ReferencesWildcardType(ctx, relationName)
				if err != nil {
					return err
				}

				if referencedWildcard != nil {
					return newErrorWithSource(childOneof, relationName, "for arrow under relation `%s`: relation `%s#%s` includes wildcard type `%s` via relation `%s`: wildcard relations cannot be used on the left side of arrows", relation.Name, nts.nsDef.Name, relationName, referencedWildcard.WildcardType.GetNamespace(), tuple.StringRR(referencedWildcard.ReferencingRelation))
				}
			}
			return nil
		})
		if rerr != nil {
			return nil, rerr.(error)
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
				return nil, newErrorWithSource(relation, relation.Name, "at least one allowed relation/permission is required in relation `%s`", relation.Name)
			}
		} else {
			if len(allowedDirectRelations) != 0 {
				return nil, newErrorWithSource(relation, relation.Name, "direct relations are not allowed under relation `%s`", relation.Name)
			}
		}

		// Verify that all allowed relations are not this very relation, and that
		// they exist within the namespace.
		for _, allowedRelation := range allowedDirectRelations {
			if allowedRelation.GetNamespace() == nts.nsDef.Name {
				if allowedRelation.GetPublicWildcard() == nil && allowedRelation.GetRelation() != tuple.Ellipsis {
					_, ok := nts.relationMap[allowedRelation.GetRelation()]
					if !ok {
						return nil, newErrorWithSource(allowedRelation, allowedRelation.GetRelation(), "for relation `%s`: relation/permission `%s` was not found under definition `%s`", relation.Name, allowedRelation.GetRelation(), allowedRelation.GetNamespace())
					}
				}
			} else {
				subjectTS, err := nts.typeSystemForNamespace(ctx, allowedRelation.GetNamespace())
				if err != nil {
					return nil, newErrorWithSource(allowedRelation, allowedRelation.GetNamespace(), "could not lookup definition `%s` for relation `%s`: %w", allowedRelation.GetNamespace(), relation.Name, err)
				}

				if allowedRelation.GetPublicWildcard() == nil && allowedRelation.GetRelation() != tuple.Ellipsis {
					// Ensure the relation exists.
					ok := subjectTS.HasRelation(allowedRelation.GetRelation())
					if !ok {
						return nil, newErrorWithSource(allowedRelation, allowedRelation.GetRelation(), "for relation `%s`: relation/permission `%s` was not found under definition `%s`", relation.Name, allowedRelation.GetRelation(), allowedRelation.GetNamespace())
					}

					// Ensure the relation doesn't itself import wildcard.
					referencedWildcard, err := subjectTS.ReferencesWildcardType(ctx, allowedRelation.GetRelation())
					if err != nil {
						return nil, err
					}

					if referencedWildcard != nil {
						return nil, newErrorWithSource(allowedRelation, allowedRelation.GetNamespace()+"#"+allowedRelation.GetRelation(), "for relation `%s`: relation/permission `%s#%s` includes wildcard type `%s` via relation `%s`: wildcard relations cannot be transitively included", relation.Name, allowedRelation.GetNamespace(), allowedRelation.GetRelation(), referencedWildcard.WildcardType.GetNamespace(), tuple.StringRR(referencedWildcard.ReferencingRelation))
					}
				}
			}
		}
	}

	return &ValidatedNamespaceTypeSystem{nts}, nil
}

func (nts *TypeSystem) typeSystemForNamespace(ctx context.Context, namespaceName string) (*TypeSystem, error) {
	if nts.nsDef.Name == namespaceName {
		return nts, nil
	}

	nsDef, err := nts.lookupNamespace(ctx, namespaceName)
	if err != nil {
		return nil, err
	}

	return BuildNamespaceTypeSystem(nsDef, nts.lookupNamespace)
}

// ValidatedNamespaceTypeSystem is validated type system for a namespace.
type ValidatedNamespaceTypeSystem struct {
	*TypeSystem
}
