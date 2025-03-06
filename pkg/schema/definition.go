package schema

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
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

// AllowedDefinitionOption indicates whether an allowed definition of a particular kind is allowed on the right side of another relation.
type AllowedDefinitionOption int

const (
	// UnknownIfAllowedDefinition indicates that no type information is defined for
	// this relation.
	UnknownIfAllowedDefinition AllowedDefinitionOption = iota

	// AllowedDefinitionValid indicates that the specified subject definition is valid.
	AllowedDefinitionValid

	// AllowedDefinitionNotValid indicates that the specified subject definition is not valid.
	AllowedDefinitionNotValid
)

// NewDefinition returns a new type definition for the given definition proto.
func NewDefinition(ts *TypeSystem, nsDef *core.NamespaceDefinition) (*Definition, error) {
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

	return &Definition{
		ts:          ts,
		nsDef:       nsDef,
		relationMap: relationMap,
	}, nil
}

// Definition represents typing information found in a definition.
// It also provides better ergonomic accessors to the defintion's type information.
type Definition struct {
	ts          *TypeSystem
	nsDef       *core.NamespaceDefinition
	relationMap map[string]*core.Relation
}

// Namespace is the NamespaceDefinition for which the type system was constructed.
func (def *Definition) Namespace() *core.NamespaceDefinition {
	return def.nsDef
}

// TypeSystem returns the typesystem for this definition
func (def *Definition) TypeSystem() *TypeSystem {
	return def.ts
}

// HasTypeInformation returns true if the relation with the given name exists and has type
// information defined.
func (def *Definition) HasTypeInformation(relationName string) bool {
	rel, ok := def.relationMap[relationName]
	return ok && rel.GetTypeInformation() != nil
}

// HasRelation returns true if the definition has the given relation defined.
func (def *Definition) HasRelation(relationName string) bool {
	_, ok := def.relationMap[relationName]
	return ok
}

// GetRelation returns the relation that's defined with the give name in the type system or returns false.
func (def *Definition) GetRelation(relationName string) (*core.Relation, bool) {
	rel, ok := def.relationMap[relationName]
	return rel, ok
}

// IsPermission returns true if the definition has the given relation defined and it is
// a permission.
func (def *Definition) IsPermission(relationName string) bool {
	found, ok := def.relationMap[relationName]
	if !ok {
		return false
	}

	return nspkg.GetRelationKind(found) == iv1.RelationMetadata_PERMISSION
}

// GetAllowedDirectNamespaceSubjectRelations returns the subject relations for the target definition, if it is defined as appearing
// somewhere on the right side of a relation (except wildcards). Returns nil if there is no type information or it is not allowed.
func (def *Definition) GetAllowedDirectNamespaceSubjectRelations(sourceRelationName string, targetNamespaceName string) (*mapz.Set[string], error) {
	found, ok := def.relationMap[sourceRelationName]
	if !ok {
		return nil, asTypeError(NewRelationNotFoundErr(def.nsDef.Name, sourceRelationName))
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

// IsAllowedDirectNamespace returns whether the target definition is defined as appearing somewhere on the
// right side of a relation (except public).
func (def *Definition) IsAllowedDirectNamespace(sourceRelationName string, targetNamespaceName string) (AllowedDefinitionOption, error) {
	found, ok := def.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfAllowedDefinition, asTypeError(NewRelationNotFoundErr(def.nsDef.Name, sourceRelationName))
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return UnknownIfAllowedDefinition, nil
	}

	allowedRelations := typeInfo.GetAllowedDirectRelations()
	for _, allowedRelation := range allowedRelations {
		if allowedRelation.GetNamespace() == targetNamespaceName && allowedRelation.GetPublicWildcard() == nil {
			return AllowedDefinitionValid, nil
		}
	}

	return AllowedDefinitionNotValid, nil
}

// IsAllowedPublicNamespace returns whether the target definition is defined as public on the source relation.
func (def *Definition) IsAllowedPublicNamespace(sourceRelationName string, targetNamespaceName string) (AllowedPublicSubject, error) {
	found, ok := def.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfPublicAllowed, asTypeError(NewRelationNotFoundErr(def.nsDef.Name, sourceRelationName))
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
func (def *Definition) IsAllowedDirectRelation(sourceRelationName string, targetNamespaceName string, targetRelationName string) (AllowedDirectRelation, error) {
	found, ok := def.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfRelationAllowed, asTypeError(NewRelationNotFoundErr(def.nsDef.Name, sourceRelationName))
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
func (def *Definition) HasAllowedRelation(sourceRelationName string, toCheck *core.AllowedRelation) (AllowedRelationOption, error) {
	found, ok := def.relationMap[sourceRelationName]
	if !ok {
		return UnknownIfAllowed, asTypeError(NewRelationNotFoundErr(def.nsDef.Name, sourceRelationName))
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
func (def *Definition) AllowedDirectRelationsAndWildcards(sourceRelationName string) ([]*core.AllowedRelation, error) {
	found, ok := def.relationMap[sourceRelationName]
	if !ok {
		return []*core.AllowedRelation{}, asTypeError(NewRelationNotFoundErr(def.nsDef.Name, sourceRelationName))
	}

	typeInfo := found.GetTypeInformation()
	if typeInfo == nil {
		return []*core.AllowedRelation{}, nil
	}

	return typeInfo.GetAllowedDirectRelations(), nil
}

// AllowedSubjectRelations returns the allowed subject relations for a source relation. Note that this function will *not*
// return wildcards, and returns without the marked caveats and expiration.
func (def *Definition) AllowedSubjectRelations(sourceRelationName string) ([]*core.RelationReference, error) {
	allowedDirect, err := def.AllowedDirectRelationsAndWildcards(sourceRelationName)
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

// RelationDoesNotAllowCaveatsOrTraitsForSubject returns true if and only if it can be conclusively determined that
// the given subject type does not accept any caveats or traits on the given relation. If the relation does not have type information,
// returns an error.
func (def *Definition) RelationDoesNotAllowCaveatsOrTraitsForSubject(relationName string, subjectTypeName string) (bool, error) {
	relation, ok := def.relationMap[relationName]
	if !ok {
		return false, NewRelationNotFoundErr(def.nsDef.Name, relationName)
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
