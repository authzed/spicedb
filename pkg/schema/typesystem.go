package schema

import (
	"context"
	"sync"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

// GetValidatedDefinition looks up and returns a definition struct, if it has been validated.
func (ts *TypeSystem) GetDeprecationForNamespace(ctx context.Context, definition string) (*core.Deprecation, bool, error) {
	ns, _, err := ts.resolver.LookupDefinition(ctx, definition)
	if err != nil {
		return nil, false, err
	}

	if ns.Deprecation != nil && ns.Deprecation.DeprecationType != core.DeprecationType_DEPRECATED_TYPE_UNSPECIFIED {
		return ns.Deprecation, true, nil
	}

	return &core.Deprecation{}, false, nil
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
func (ts *TypeSystem) GetDeprecationForAllowedRelation(
	ctx context.Context,
	resourceNamespace string,
	resourceRelation string,
	subjectNamespace string,
	subjectRelation string,
	isWildcard bool,
) (*core.Deprecation, bool, error) {
	def, _, err := ts.getDefinition(ctx, resourceNamespace)
	if err != nil {
		return nil, false, err
	}

	rel, ok := def.GetRelation(resourceRelation)
	if !ok {
		return nil, false, nil
	}

	if rel.TypeInformation == nil || rel.TypeInformation.GetAllowedDirectRelations() == nil {
		return nil, false, nil
	}

	for _, allowed := range rel.TypeInformation.GetAllowedDirectRelations() {
		// check namespace match
		if allowed.Namespace != subjectNamespace {
			continue
		}

		switch w := allowed.RelationOrWildcard.(type) {
		case *core.AllowedRelation_PublicWildcard_:
			if !isWildcard {
				continue
			}
		case *core.AllowedRelation_Relation:
			if isWildcard || w.Relation != subjectRelation {
				continue
			}
		}

		if dep := allowed.RequiredDeprecation; dep != nil &&
			dep.DeprecationType != core.DeprecationType_DEPRECATED_TYPE_UNSPECIFIED {
			return dep, true, nil
		}
	}
	return nil, false, nil
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
