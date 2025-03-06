package schema

import (
	"context"

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
	definitions        map[string]*Definition
	resolver           Resolver
	wildcardCheckCache map[string]*WildcardTypeReference
}

// NewTypeSystem builds a TypeSystem object from a resolver, which can look up the definitions.
func NewTypeSystem(resolver Resolver) *TypeSystem {
	return &TypeSystem{
		definitions:        make(map[string]*Definition),
		resolver:           resolver,
		wildcardCheckCache: nil,
	}
}

// GetDefinition looks up and returns a definition struct.
func (ts *TypeSystem) GetDefinition(ctx context.Context, definition string) (*Definition, error) {
	if v, ok := ts.definitions[definition]; ok {
		return v, nil
	}
	ns, _, err := ts.resolver.LookupDefinition(ctx, definition)
	if err != nil {
		return nil, err
	}
	d, err := NewDefinition(ts, ns)
	if err != nil {
		return nil, err
	}
	ts.definitions[definition] = d
	return d, nil
}
