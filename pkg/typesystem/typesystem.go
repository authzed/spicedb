package typesystem

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/graph"
	nspkg "github.com/authzed/spicedb/pkg/namespace"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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

// AllowedRelationOption indicates whether an allowed relation of a particular kind is allowed on the right side of another relation.
type AllowedRelationOption int

const (
	// UnknownIfAllowed indicates that no type information is defined for
	// this relation.
	UnknownIfAllowed AllowedRelationOption = iota

	// AllowedRelationValid indicates that the specified subject relation is valid.
	AllowedRelationValid

	// AllowedRelationNotValid indicates that the specified subject relation is not valid.
	AllowedRelationNotValid
)

// AllowedNamespaceOption indicates whether an allowed namespace of a particular kind is allowed on the right side of another relation.
type AllowedNamespaceOption int

const (
	// UnknownIfAllowedNamespace indicates that no type information is defined for
	// this relation.
	UnknownIfAllowedNamespace AllowedNamespaceOption = iota

	// AllowedNamespaceValid indicates that the specified subject namespace is valid.
	AllowedNamespaceValid

	// AllowedNamespaceNotValid indicates that the specified subject namespace is not valid.
	AllowedNamespaceNotValid
)

// NewNamespaceTypeSystem returns a new type system for the given namespace. Note that the type
// system is not validated until Validate is called.
func NewNamespaceTypeSystem(nsDef *core.NamespaceDefinition, resolver Resolver) (*TypeSystem, error) {
	relationMap := make(map[string]*core.Relation, len(nsDef.GetRelation()))
	for _, relation := range nsDef.GetRelation() {
		_, existing := relationMap[relation.Name]
		if existing {
			return nil, NewTypeWithSourceError(
				NewDuplicateRelationError(nsDef.Name, relation.Name),
				relation,
				relation.Name,
			)
		}

		relationMap[relation.Name] = relation
	}

	return &TypeSystem{
		resolver:           resolver,
		nsDef:              nsDef,
		relationMap:        relationMap,
		wildcardCheckCache: nil,
	}, nil
}

// TypeSystem represents typing information found in a namespace.
type TypeSystem struct {
	resolver           Resolver
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

// GetRelation returns the relation that's defined with the give name in the type system or returns false.
func (nts *TypeSystem) GetRelation(relationName string) (*core.Relation, bool) {
	rel, ok := nts.relationMap[relationName]
	return rel, ok
}

// MustGetRelation returns the relation that's defined with the give name in the type system or panics.
func (nts *TypeSystem) MustGetRelation(relationName string) *core.Relation {
	rel, ok := nts.relationMap[relationName]
	if !ok {
		panic("Missing relation")
	}
	return rel
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

// GetAllowedDirectNamespaceSubjectRelations returns the subject relations for the target namespace, if it is defined as appearing
// somewhere on the right side of a relation (except public). Returns nil if there is no type information or it is not allowed.
func (nts *TypeSystem) GetAllowedDirectNamespaceSubjectRelations(sourceRelationName string, targetNamespaceName string) (*mapz.Set[string], error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return nil, asTypeError(NewRelationNotFoundErr(nts.nsDef.Name, sourceRelationName))
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return nil, nil
	}

	allowedRelations := typeInfo.GetAllowedDirectRelations()
	allowedSubjectRelations := mapz.NewSet[string]()
	for _, allowedRelation := range allowedRelations {
		if allowedRelation.GetNamespace() == targetNamespaceName && allowedRelation.GetPublicWildcard() == nil {
			allowedSubjectRelations.Add(allowedRelation.GetRelation())
		}
	}

	return allowedSubjectRelations, nil
}

// IsAllowedDirectNamespace returns whether the target namespace is defined as appearing somewhere on the
// right side of a relation (except public).
func (nts *TypeSystem) IsAllowedDirectNamespace(sourceRelationName string, targetNamespaceName string) (AllowedNamespaceOption, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfAllowedNamespace, asTypeError(NewRelationNotFoundErr(nts.nsDef.Name, sourceRelationName))
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return UnknownIfAllowedNamespace, nil
	}

	allowedRelations := typeInfo.GetAllowedDirectRelations()
	for _, allowedRelation := range allowedRelations {
		if allowedRelation.GetNamespace() == targetNamespaceName && allowedRelation.GetPublicWildcard() == nil {
			return AllowedNamespaceValid, nil
		}
	}

	return AllowedNamespaceNotValid, nil
}

// IsAllowedPublicNamespace returns whether the target namespace is defined as public on the source relation.
func (nts *TypeSystem) IsAllowedPublicNamespace(sourceRelationName string, targetNamespaceName string) (AllowedPublicSubject, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfPublicAllowed, asTypeError(NewRelationNotFoundErr(nts.nsDef.Name, sourceRelationName))
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
		return UnknownIfRelationAllowed, asTypeError(NewRelationNotFoundErr(nts.nsDef.Name, sourceRelationName))
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

// HasAllowedRelation returns whether the source relation has the given allowed relation.
func (nts *TypeSystem) HasAllowedRelation(sourceRelationName string, toCheck *core.AllowedRelation) (AllowedRelationOption, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfAllowed, asTypeError(NewRelationNotFoundErr(nts.nsDef.Name, sourceRelationName))
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return UnknownIfAllowed, nil
	}

	allowedRelations := typeInfo.GetAllowedDirectRelations()
	for _, allowedRelation := range allowedRelations {
		if SourceForAllowedRelation(allowedRelation) == SourceForAllowedRelation(toCheck) {
			return AllowedRelationValid, nil
		}
	}

	return AllowedRelationNotValid, nil
}

// AllowedDirectRelationsAndWildcards returns the allowed subject relations for a source relation.
// Note that this function will return wildcards.
func (nts *TypeSystem) AllowedDirectRelationsAndWildcards(sourceRelationName string) ([]*core.AllowedRelation, error) {
	found, ok := nts.relationMap[sourceRelationName]
	if !ok {
		return []*core.AllowedRelation{}, asTypeError(NewRelationNotFoundErr(nts.nsDef.Name, sourceRelationName))
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return []*core.AllowedRelation{}, nil
	}

	return typeInfo.GetAllowedDirectRelations(), nil
}

// AllowedSubjectRelations returns the allowed subject relations for a source relation. Note that this function will *not*
// return wildcards, and returns without the marked caveats and expiration.
func (nts *TypeSystem) AllowedSubjectRelations(sourceRelationName string) ([]*core.RelationReference, error) {
	allowedDirect, err := nts.AllowedDirectRelationsAndWildcards(sourceRelationName)
	if err != nil {
		return []*core.RelationReference{}, asTypeError(err)
	}

	filtered := make([]*core.RelationReference, 0, len(allowedDirect))
	for _, allowed := range allowedDirect {
		if allowed.GetPublicWildcard() != nil {
			continue
		}

		if allowed.GetRelation() == "" {
			return nil, spiceerrors.MustBugf("got an empty relation for a non-wildcard type definition under namespace")
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

// referencesWildcardType returns true if the relation references a wildcard type, either directly or via
// another relation.
func (nts *TypeSystem) referencesWildcardType(ctx context.Context, relationName string) (*WildcardTypeReference, error) {
	return nts.referencesWildcardTypeWithEncountered(ctx, relationName, map[string]bool{})
}

func (nts *TypeSystem) referencesWildcardTypeWithEncountered(ctx context.Context, relationName string, encountered map[string]bool) (*WildcardTypeReference, error) {
	if nts.wildcardCheckCache == nil {
		nts.wildcardCheckCache = make(map[string]*WildcardTypeReference, 1)
	}

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
	relString := tuple.JoinRelRef(nts.nsDef.Name, relationName)
	if _, ok := encountered[relString]; ok {
		return nil, nil
	}
	encountered[relString] = true

	allowedRels, err := nts.AllowedDirectRelationsAndWildcards(relationName)
	if err != nil {
		return nil, asTypeError(err)
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
				found, err := nts.referencesWildcardTypeWithEncountered(ctx, allowedRelation.GetRelation(), encountered)
				if err != nil {
					return nil, asTypeError(err)
				}

				if found != nil {
					return found, nil
				}
				continue
			}

			subjectTS, err := nts.TypeSystemForNamespace(ctx, allowedRelation.GetNamespace())
			if err != nil {
				return nil, asTypeError(err)
			}

			found, err := subjectTS.referencesWildcardTypeWithEncountered(ctx, allowedRelation.GetRelation(), encountered)
			if err != nil {
				return nil, asTypeError(err)
			}

			if found != nil {
				return found, nil
			}
		}
	}

	return nil, nil
}

// Validate runs validation on the type system for the namespace to ensure it is consistent.
func (nts *TypeSystem) Validate(ctx context.Context) (*ValidatedNamespaceTypeSystem, error) {
	for _, relation := range nts.relationMap {
		relation := relation

		// Validate the usersets's.
		usersetRewrite := relation.GetUsersetRewrite()
		rerr, err := graph.WalkRewrite(usersetRewrite, func(childOneof *core.SetOperation_Child) (interface{}, error) {
			switch child := childOneof.ChildType.(type) {
			case *core.SetOperation_Child_ComputedUserset:
				relationName := child.ComputedUserset.GetRelation()
				_, ok := nts.relationMap[relationName]
				if !ok {
					return NewTypeWithSourceError(
						NewRelationNotFoundErr(nts.nsDef.Name, relationName),
						childOneof,
						relationName,
					), nil
				}

			case *core.SetOperation_Child_TupleToUserset:
				ttu := child.TupleToUserset
				if ttu == nil {
					return nil, nil
				}

				tupleset := ttu.GetTupleset()
				if tupleset == nil {
					return nil, nil
				}

				relationName := tupleset.GetRelation()
				found, ok := nts.relationMap[relationName]
				if !ok {
					return NewTypeWithSourceError(
						NewRelationNotFoundErr(nts.nsDef.Name, relationName),
						childOneof,
						relationName,
					), nil
				}

				if nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION {
					return NewTypeWithSourceError(
						NewPermissionUsedOnLeftOfArrowErr(nts.nsDef.Name, relation.Name, relationName),
						childOneof, relationName), nil
				}

				// Ensure the tupleset relation doesn't itself import wildcard.
				referencedWildcard, err := nts.referencesWildcardType(ctx, relationName)
				if err != nil {
					return err, nil
				}

				if referencedWildcard != nil {
					return NewTypeWithSourceError(
						NewWildcardUsedInArrowErr(
							nts.nsDef.Name,
							relation.Name,
							relationName,
							referencedWildcard.WildcardType.GetNamespace(),
							tuple.StringCoreRR(referencedWildcard.ReferencingRelation),
						),
						childOneof, relationName,
					), nil
				}

			case *core.SetOperation_Child_FunctionedTupleToUserset:
				ttu := child.FunctionedTupleToUserset
				if ttu == nil {
					return nil, nil
				}

				tupleset := ttu.GetTupleset()
				if tupleset == nil {
					return nil, nil
				}

				relationName := tupleset.GetRelation()
				found, ok := nts.relationMap[relationName]
				if !ok {
					return NewTypeWithSourceError(
						NewRelationNotFoundErr(nts.nsDef.Name, relationName),
						childOneof,
						relationName,
					), nil
				}

				if nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION {
					return NewTypeWithSourceError(
						NewPermissionUsedOnLeftOfArrowErr(nts.nsDef.Name, relation.Name, relationName),
						childOneof, relationName), nil
				}

				// Ensure the tupleset relation doesn't itself import wildcard.
				referencedWildcard, err := nts.referencesWildcardType(ctx, relationName)
				if err != nil {
					return err, nil
				}

				if referencedWildcard != nil {
					return NewTypeWithSourceError(
						NewWildcardUsedInArrowErr(
							nts.nsDef.Name,
							relation.Name,
							relationName,
							referencedWildcard.WildcardType.GetNamespace(),
							tuple.StringCoreRR(referencedWildcard.ReferencingRelation),
						),
						childOneof, relationName,
					), nil
				}
			}
			return nil, nil
		})
		if rerr != nil {
			return nil, asTypeError(rerr.(error))
		}
		if err != nil {
			return nil, err
		}

		// Validate type information.
		typeInfo := relation.TypeInformation
		if typeInfo == nil {
			continue
		}

		allowedDirectRelations := typeInfo.GetAllowedDirectRelations()

		// Check for a _this or the lack of a userset_rewrite. If either is found,
		// then the allowed list must have at least one type.
		hasThis, err := graph.HasThis(usersetRewrite)
		if err != nil {
			return nil, err
		}

		if usersetRewrite == nil || hasThis {
			if len(allowedDirectRelations) == 0 {
				return nil, NewTypeWithSourceError(
					NewMissingAllowedRelationsErr(nts.nsDef.Name, relation.Name),
					relation, relation.Name,
				)
			}
		} else {
			if len(allowedDirectRelations) != 0 {
				// NOTE: This is a legacy error and should never really occur with schema.
				return nil, NewTypeWithSourceError(
					fmt.Errorf("direct relations are not allowed under relation `%s`", relation.Name),
					relation, relation.Name)
			}
		}

		// Allowed relations verification:
		// 1) that all allowed relations are not this very relation
		// 2) that they exist within the referenced namespace
		// 3) that they are not duplicated in any way
		// 4) that if they have a caveat reference, the caveat is valid
		encountered := mapz.NewSet[string]()

		for _, allowedRelation := range allowedDirectRelations {
			source := SourceForAllowedRelation(allowedRelation)
			if !encountered.Add(source) {
				return nil, NewTypeWithSourceError(
					NewDuplicateAllowedRelationErr(nts.nsDef.Name, relation.Name, source),
					allowedRelation,
					source,
				)
			}

			// Check the namespace.
			if allowedRelation.GetNamespace() == nts.nsDef.Name {
				if allowedRelation.GetPublicWildcard() == nil && allowedRelation.GetRelation() != tuple.Ellipsis {
					_, ok := nts.relationMap[allowedRelation.GetRelation()]
					if !ok {
						return nil, NewTypeWithSourceError(
							NewRelationNotFoundErr(allowedRelation.GetNamespace(), allowedRelation.GetRelation()),
							allowedRelation,
							allowedRelation.GetRelation(),
						)
					}
				}
			} else {
				subjectTS, err := nts.TypeSystemForNamespace(ctx, allowedRelation.GetNamespace())
				if err != nil {
					return nil, NewTypeWithSourceError(
						fmt.Errorf("could not lookup definition `%s` for relation `%s`: %w", allowedRelation.GetNamespace(), relation.Name, err),
						allowedRelation,
						allowedRelation.GetNamespace(),
					)
				}

				// Check for relations.
				if allowedRelation.GetPublicWildcard() == nil && allowedRelation.GetRelation() != tuple.Ellipsis {
					// Ensure the relation exists.
					ok := subjectTS.HasRelation(allowedRelation.GetRelation())
					if !ok {
						return nil, NewTypeWithSourceError(
							NewRelationNotFoundErr(allowedRelation.GetNamespace(), allowedRelation.GetRelation()),
							allowedRelation,
							allowedRelation.GetRelation(),
						)
					}

					// Ensure the relation doesn't itself import wildcard.
					referencedWildcard, err := subjectTS.referencesWildcardType(ctx, allowedRelation.GetRelation())
					if err != nil {
						return nil, err
					}

					if referencedWildcard != nil {
						return nil, NewTypeWithSourceError(
							NewTransitiveWildcardErr(
								nts.nsDef.Name,
								relation.GetName(),
								allowedRelation.Namespace,
								allowedRelation.GetRelation(),
								referencedWildcard.WildcardType.GetNamespace(),
								tuple.StringCoreRR(referencedWildcard.ReferencingRelation),
							),
							allowedRelation,
							tuple.JoinRelRef(allowedRelation.GetNamespace(), allowedRelation.GetRelation()),
						)
					}
				}
			}

			// Check the caveat, if any.
			if allowedRelation.GetRequiredCaveat() != nil {
				_, err := nts.resolver.LookupCaveat(ctx, allowedRelation.GetRequiredCaveat().CaveatName)
				if err != nil {
					return nil, NewTypeWithSourceError(
						fmt.Errorf("could not lookup caveat `%s` for relation `%s`: %w", allowedRelation.GetRequiredCaveat().CaveatName, relation.Name, err),
						allowedRelation,
						source,
					)
				}
			}
		}
	}

	return &ValidatedNamespaceTypeSystem{nts}, nil
}

// SourceForAllowedRelation returns the source code representation of an allowed relation.
func SourceForAllowedRelation(allowedRelation *core.AllowedRelation) string {
	caveatAndTraitsStr := ""

	hasCaveat := allowedRelation.GetRequiredCaveat() != nil
	hasExpirationTrait := allowedRelation.GetRequiredExpiration() != nil
	hasTraits := hasCaveat || hasExpirationTrait

	if hasTraits {
		caveatAndTraitsStr = " with "
		if hasCaveat {
			caveatAndTraitsStr += allowedRelation.RequiredCaveat.CaveatName
		}

		if hasCaveat && hasExpirationTrait {
			caveatAndTraitsStr += " and "
		}

		if hasExpirationTrait {
			caveatAndTraitsStr += "expiration"
		}
	}

	if allowedRelation.GetPublicWildcard() != nil {
		return tuple.JoinObjectRef(allowedRelation.Namespace, "*") + caveatAndTraitsStr
	}

	if rel := allowedRelation.GetRelation(); rel != tuple.Ellipsis {
		return tuple.JoinRelRef(allowedRelation.Namespace, rel) + caveatAndTraitsStr
	}

	return allowedRelation.Namespace + caveatAndTraitsStr
}

// TypeSystemForNamespace returns a type system for the given namespace.
func (nts *TypeSystem) TypeSystemForNamespace(ctx context.Context, namespaceName string) (*TypeSystem, error) {
	if nts.nsDef.Name == namespaceName {
		return nts, nil
	}

	nsDef, err := nts.resolver.LookupNamespace(ctx, namespaceName)
	if err != nil {
		return nil, err
	}

	return NewNamespaceTypeSystem(nsDef, nts.resolver)
}

// RelationDoesNotAllowCaveatsOrTraitsForSubject returns true if and only if it can be conclusively determined that
// the given subject type does not accept any caveats or traits on the given relation. If the relation does not have type information,
// returns an error.
func (nts *TypeSystem) RelationDoesNotAllowCaveatsOrTraitsForSubject(relationName string, subjectTypeName string) (bool, error) {
	relation, ok := nts.relationMap[relationName]
	if !ok {
		return false, NewRelationNotFoundErr(nts.nsDef.Name, relationName)
	}

	typeInfo := relation.GetTypeInformation()
	if typeInfo == nil {
		return false, NewTypeWithSourceError(
			fmt.Errorf("relation `%s` does not have type information", relationName),
			relation, relationName,
		)
	}

	foundSubjectType := false
	for _, allowedRelation := range typeInfo.GetAllowedDirectRelations() {
		if allowedRelation.GetNamespace() == subjectTypeName {
			foundSubjectType = true
			if allowedRelation.GetRequiredCaveat() != nil && allowedRelation.GetRequiredCaveat().CaveatName != "" {
				return false, nil
			}
			if allowedRelation.GetRequiredExpiration() != nil {
				return false, nil
			}
		}
	}

	if !foundSubjectType {
		return false, NewTypeWithSourceError(
			fmt.Errorf("relation `%s` does not allow subject type `%s`", relationName, subjectTypeName),
			relation, relationName,
		)
	}

	return true, nil
}

// ValidatedNamespaceTypeSystem is validated type system for a namespace.
type ValidatedNamespaceTypeSystem struct {
	*TypeSystem
}

// NewTypeWithSourceError creates a new type error at the specific position and with source code, wrapping the underlying
// error.
func NewTypeWithSourceError(wrapped error, withSource nspkg.WithSourcePosition, sourceCodeString string) error {
	sourcePosition := withSource.GetSourcePosition()
	if sourcePosition != nil {
		return asTypeError(spiceerrors.NewWithSourceError(
			wrapped,
			sourceCodeString,
			sourcePosition.ZeroIndexedLineNumber+1, // +1 to make 1-indexed
			sourcePosition.ZeroIndexedColumnPosition+1, // +1 to make 1-indexed
		))
	}

	return asTypeError(spiceerrors.NewWithSourceError(
		wrapped,
		sourceCodeString,
		0,
		0,
	))
}

// ReadNamespaceAndTypes reads a namespace definition, version, and type system and returns it if found.
func ReadNamespaceAndTypes(
	ctx context.Context,
	nsName string,
	ds datastore.Reader,
) (*core.NamespaceDefinition, *ValidatedNamespaceTypeSystem, error) {
	nsDef, _, err := ds.ReadNamespaceByName(ctx, nsName)
	if err != nil {
		return nil, nil, err
	}

	ts, terr := NewNamespaceTypeSystem(nsDef, ResolverForDatastoreReader(ds))
	if terr != nil {
		return nil, nil, terr
	}

	// NOTE: since the type system was read from the datastore, it must have been validated
	// on the way in, so it is safe for us to return it as a validated type system.
	return nsDef, &ValidatedNamespaceTypeSystem{ts}, nil
}
