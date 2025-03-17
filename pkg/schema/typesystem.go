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
	validatedDefinitions map[string]*ValidatedDefinition
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
