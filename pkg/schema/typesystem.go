package schema

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

type (
	// Caveat is an alias for a core.CaveatDefinition proto
	Caveat = core.CaveatDefinition
	// Relation is an alias for a core.Relation proto
	Relation = core.Relation
)

// TypeSystem is a cache and view into an entire combined schema of type definitions and caveats.
// It also provides accessors to build reachability graphs for the underlying types.
type TypeSystem struct {
	sync.Mutex
	validatedDefinitions map[string]*ValidatedDefinition // GUARDED_BY(Mutex)
	resolver             Resolver
	wildcardCheckCache   map[string]*WildcardTypeReference
}

// AllowedRelationTraits represents the characteristics of an allowed relation to be used for checking deprecation status.
type AllowedRelationTraits struct {
	ResourceNamespace string
	ResourceRelation  string
	SubjectNamespace  string
	SubjectRelation   string
	IsWildcard        bool
	HasCaveat         bool
	HasExpiration     bool
}

// NewTypeSystem builds a TypeSystem object from a resolver, which can look up the definitions.
func NewTypeSystem(resolver Resolver) *TypeSystem {
	return &TypeSystem{
		validatedDefinitions: make(map[string]*ValidatedDefinition),
		resolver:             resolver,
		wildcardCheckCache:   nil,
	}
}

// GetDefinition looks up and returns a definition struct.
func (ts *TypeSystem) GetDefinition(ctx context.Context, definition string) (*Definition, error) {
	v, _, err := ts.getDefinition(ctx, definition)
	return v, err
}

// GetDeprecationForObjectType looks up and returns deprecation attached to a object type.
func (ts *TypeSystem) GetDeprecationForObjectType(ctx context.Context, definition string) (*core.Deprecation, bool, error) {
	ns, _, err := ts.resolver.LookupDefinition(ctx, definition)
	if err != nil {
		return nil, false, err
	}

	if ns.Deprecation != nil && ns.Deprecation.DeprecationType != core.DeprecationType_DEPRECATED_TYPE_UNSPECIFIED {
		return ns.Deprecation, true, nil
	}

	return nil, false, nil
}

// GetDeprecationForRelation returns the deprecation attached to a relation.
func (ts *TypeSystem) GetDeprecationForRelation(ctx context.Context, namespace string, relation string) (*core.Deprecation, bool, error) {
	ns, _, err := ts.resolver.LookupDefinition(ctx, namespace)
	if err != nil {
		return nil, false, err
	}

	d, err := NewDefinition(ts, ns)
	if err != nil {
		return nil, false, err
	}

	rel := d.relationMap[relation]

	if rel != nil && rel.Deprecation != nil && rel.Deprecation.DeprecationType != core.DeprecationType_DEPRECATED_TYPE_UNSPECIFIED {
		return rel.Deprecation, true, nil
	}

	return &core.Deprecation{}, false, nil
}

// GetDeprecationForAllowedRelation returns the deprecation attached to AllowedRelation.
func (ts *TypeSystem) GetDeprecationForAllowedRelation(ctx context.Context, traits AllowedRelationTraits) (*core.Deprecation, bool, error) {
	def, _, err := ts.getDefinition(ctx, traits.ResourceNamespace)
	if err != nil {
		return nil, false, err
	}

	rel, ok := def.GetRelation(traits.ResourceRelation)
	if !ok {
		return nil, false, nil
	}

	if rel.TypeInformation == nil || rel.TypeInformation.GetAllowedDirectRelations() == nil {
		return nil, false, nil
	}

	for _, allowed := range rel.TypeInformation.GetAllowedDirectRelations() {
		// check namespace match
		if allowed.Namespace != traits.SubjectNamespace {
			continue
		}

		// check for caveat match
		if traits.HasCaveat != (allowed.RequiredCaveat != nil) {
			continue
		}

		// check for expiration match
		if traits.HasExpiration != (allowed.RequiredExpiration != nil) {
			continue
		}

		switch w := allowed.RelationOrWildcard.(type) {
		case *core.AllowedRelation_PublicWildcard_:
			if !traits.IsWildcard {
				continue
			}
		case *core.AllowedRelation_Relation:
			if traits.IsWildcard || w.Relation != traits.SubjectRelation {
				continue
			}
		default:
			return nil, false, spiceerrors.MustBugf("unknown type for RelationOrWildcard: %T", allowed.RelationOrWildcard)
		}

		if dep := allowed.Deprecation; dep != nil &&
			dep.DeprecationType != core.DeprecationType_DEPRECATED_TYPE_UNSPECIFIED {
			return dep, true, nil
		}
	}
	return nil, false, nil
}

// CheckRelationshipDeprecation performs a check over the deprecated relationship's resource, relation, subject and allowed relation, if any
func (ts *TypeSystem) CheckRelationshipDeprecation(ctx context.Context, relationship tuple.Relationship) error {
	// Validate if the resource relation is deprecated
	relDep, ok, err := ts.GetDeprecationForRelation(ctx, relationship.Resource.ObjectType, relationship.Resource.Relation)
	if err != nil {
		return err
	}
	if ok {
		if err := logOrErrorOnDeprecation(relationship.Resource.ObjectType, relationship.Resource.Relation, relDep, "write to deprecated relation", "", false); err != nil {
			return err
		}
	}

	// Validate if the resource object is deprecated
	resDep, ok, err := ts.GetDeprecationForObjectType(ctx, relationship.Resource.ObjectType)
	if err != nil {
		return err
	}
	if ok {
		if err := logOrErrorOnDeprecation(relationship.Resource.ObjectType, "", resDep, "write to deprecated object", "", false); err != nil {
			return err
		}
	}

	// Validate if the subject object is deprecated
	subDep, ok, err := ts.GetDeprecationForObjectType(ctx, relationship.Subject.ObjectType)
	if err != nil {
		return err
	}
	if ok {
		if err := logOrErrorOnDeprecation(relationship.Subject.ObjectType, "", subDep, "write to deprecated object", "", false); err != nil {
			return err
		}
	}

	hasExpiration := false
	hasCaveat := false
	if relationship.OptionalExpiration != nil {
		hasExpiration = true
	}

	if relationship.OptionalCaveat != nil {
		hasCaveat = true
	}

	traits := AllowedRelationTraits{
		ResourceNamespace: relationship.Resource.ObjectType,
		ResourceRelation:  relationship.Resource.Relation,
		SubjectNamespace:  relationship.Subject.ObjectType,
		SubjectRelation:   relationship.Subject.Relation,
		IsWildcard:        relationship.Subject.ObjectID == tuple.PublicWildcard,
		HasCaveat:         hasCaveat,
		HasExpiration:     hasExpiration,
	}
	// Check deprecation for allowed relation types
	dep, ok, err := ts.GetDeprecationForAllowedRelation(ctx, traits)
	if err != nil {
		return err
	}

	if ok {
		caveatName := ""
		if hasCaveat && relationship.OptionalCaveat != nil {
			caveatName = relationship.OptionalCaveat.CaveatName
		}
		wildCard := relationship.Subject.ObjectID
		if wildCard != tuple.PublicWildcard {
			wildCard = ""
		}
		if err := logOrErrorOnDeprecation(relationship.Subject.ObjectType, wildCard, dep, "write to deprecated relation", caveatName, hasExpiration); err != nil {
			return err
		}
	}

	return nil
}

// getDefinition is an internal helper for GetDefinition and GetValidatedDefinition
func (ts *TypeSystem) getDefinition(ctx context.Context, definition string) (*Definition, bool, error) {
	ts.Lock()
	v, ok := ts.validatedDefinitions[definition]
	ts.Unlock()
	if ok {
		return v.Definition, true, nil
	}

	ns, prevalidated, err := ts.resolver.LookupDefinition(ctx, definition)
	if err != nil {
		return nil, false, err
	}
	d, err := NewDefinition(ts, ns)
	if err != nil {
		return nil, false, err
	}
	if prevalidated {
		ts.Lock()
		if _, ok := ts.validatedDefinitions[definition]; !ok {
			ts.validatedDefinitions[definition] = &ValidatedDefinition{Definition: d}
		}
		ts.Unlock()
	}
	return d, prevalidated, nil
}

// PossibleTraitsForFilter loads the traits for the given filter, if possible. It returns
// the traits and an error. If the filter is known to be empty based on the schema,
// a KnownEmptyFilterError is returned.
//
// A filter is known to be empty if it is impossible for any relationships to match
// the filter based on the schema. For example, a filter with a resource type that
// does not exist in the schema is known to be empty.
//
// If the filter does not provide enough information to determine the traits or the
// emptiness, a full trait set is returned, erring on the side of caution.
func (ts *TypeSystem) PossibleTraitsForFilter(ctx context.Context, filter datastore.RelationshipsFilter) (Traits, error) {
	// Get the direct traits from the schema
	traits, err := ts.directPossibleTraitsForFilter(ctx, filter)
	if err != nil {
		return Traits{}, err
	}

	// Apply expiration filter adjustments
	traits, err = applyExpirationFilter(traits, filter.OptionalExpirationOption)
	return traits, err
}

// directPossibleTraitsForFilter loads the direct traits for the given filter without applying
// expiration filtering. It returns the traits and an error. If the filter is known to be empty,
// a KnownEmptyFilterError is returned.
func (ts *TypeSystem) directPossibleTraitsForFilter(ctx context.Context, filter datastore.RelationshipsFilter) (Traits, error) {
	if filter.OptionalResourceType == "" {
		// If no resource type is specified, we cannot determine the traits without loading all the namespaces,
		// which is currently very slow. Therefore, we return all traits.
		// TODO(jschorr): Change this once we load schema as a single unit.
		return Traits{
			AllowsCaveats:    true,
			AllowsExpiration: true,
		}, nil
	}

	vts, err := ts.GetValidatedDefinition(ctx, filter.OptionalResourceType)
	if err != nil {
		// If the type doesn't exist, we return a KnownEmptyFilterError.
		if errors.As(err, &DefinitionNotFoundError{}) {
			return Traits{}, NewKnownEmptyFilterErr(fmt.Sprintf("resource type '%s' does not exist in schema", filter.OptionalResourceType))
		}

		return Traits{}, err
	}

	// If there are no relations defined in the filter, return the traits for all relations.
	if filter.OptionalResourceRelation == "" {
		traits, err := vts.PossibleTraitsForAnyRelation()
		return traits, err
	}

	// Check if the relation exists.
	if _, ok := vts.GetRelation(filter.OptionalResourceRelation); !ok {
		// If the relation doesn't exist, we return a KnownEmptyFilterError.
		return Traits{}, NewKnownEmptyFilterErr(fmt.Sprintf("relation '%s' does not exist on resource type '%s'", filter.OptionalResourceRelation, filter.OptionalResourceType))
	}

	// If there is not subject filter, use the relation only.
	if len(filter.OptionalSubjectsSelectors) == 0 {
		traits, err := vts.PossibleTraitsForAnySubject(filter.OptionalResourceRelation)
		return traits, err
	}

	// Otherwise, build a combined traits set for all the subject filters.
	var combinedTraits Traits
	for _, subjectFilter := range filter.OptionalSubjectsSelectors {
		// If the subject type is not specified, we need to get the traits for any subject.
		if subjectFilter.OptionalSubjectType == "" {
			traits, err := vts.PossibleTraitsForAnySubject(filter.OptionalResourceRelation)
			return traits, err
		}

		// Otherwise, get the traits for the specific subject type and combine them.
		traits, err := vts.PossibleTraitsForSubject(filter.OptionalResourceRelation, subjectFilter.OptionalSubjectType)
		if err != nil {
			// If the subject type doesn't exist on this relation, fall back to traits for any subject
			// to err on the side of caution.
			traits, err = vts.PossibleTraitsForAnySubject(filter.OptionalResourceRelation)
			if err != nil {
				return Traits{}, err
			}
		}

		combinedTraits = combinedTraits.union(traits)
	}

	return combinedTraits, nil
}

// applyExpirationFilter adjusts traits based on the expiration filter option
func applyExpirationFilter(traits Traits, expirationOption datastore.ExpirationFilterOption) (Traits, error) {
	switch expirationOption {
	case datastore.ExpirationFilterOptionNone:
		// No filtering, return traits as-is
		return traits, nil

	case datastore.ExpirationFilterOptionHasExpiration:
		// Only relationships WITH expiration are allowed
		if !traits.AllowsExpiration {
			// If the relation doesn't support expiration, the filter is empty
			return Traits{}, NewKnownEmptyFilterErr("the filter requested relationships with expiration but the filter relation does not support expiration")
		}

		// Force AllowsExpiration to true since we only want expiring relationships
		return Traits{
			AllowsCaveats:    traits.AllowsCaveats,
			AllowsExpiration: true,
		}, nil

	case datastore.ExpirationFilterOptionNoExpiration:
		// Only relationships WITHOUT expiration are allowed
		// Force AllowsExpiration to false since we don't want expiring relationships
		return Traits{
			AllowsCaveats:    traits.AllowsCaveats,
			AllowsExpiration: false,
		}, nil

	default:
		// Unknown option, return traits as-is
		return traits, nil
	}
}

func logOrErrorOnDeprecation(objectType, relation string, dep *core.Deprecation, logMessage string, caveatName string, hasExpiration bool) error {
	extra := ""
	if caveatName != "" {
		extra += " with caveat `" + caveatName + "`"
	}
	if hasExpiration {
		if extra != "" {
			extra += " and expiration"
		} else {
			extra += " with expiration"
		}
	}

	switch dep.DeprecationType {
	case core.DeprecationType_DEPRECATED_TYPE_WARNING:
		msg := fmt.Sprintf("%s%s", logMessage, extra)
		if relation != "" {
			log.Warn().Str("resource_type", objectType).Str("relation", relation).Str("comments", dep.Comments).Msg(msg)
		} else {
			log.Warn().Str("resource_type", objectType).Str("comments", dep.Comments).Msg(msg)
		}

	case core.DeprecationType_DEPRECATED_TYPE_ERROR:
		switch {
		case relation == "*":
			return fmt.Errorf("wildcard allowed type %s:*%s is deprecated: %s", objectType, extra, dep.Comments)
		case relation == "" && dep.Comments != "":
			return fmt.Errorf("resource_type %s%s is deprecated: %s", objectType, extra, dep.Comments)
		case relation == "":
			return fmt.Errorf("resource_type %s%s has been marked as deprecated", objectType, extra)
		case dep.Comments != "":
			return fmt.Errorf("relation %s#%s%s is deprecated: %s", objectType, relation, extra, dep.Comments)
		default:
			return fmt.Errorf("relation %s#%s%s has been marked as deprecated", objectType, relation, extra)
		}
	}
	return nil
}
