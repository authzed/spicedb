package schema

import (
	"context"
)

type TypeSystem struct {
	definitions        map[string]*Definition
	resolver           Resolver
	wildcardCheckCache map[string]*WildcardTypeReference
}

func NewTypeSystem(resolver Resolver) *TypeSystem {
	return &TypeSystem{
		definitions:        make(map[string]*Definition),
		resolver:           resolver,
		wildcardCheckCache: nil,
	}
}

func (ts *TypeSystem) GetDefinition(ctx context.Context, definition string) (*Definition, error) {
	if v, ok := ts.definitions[definition]; ok {
		return v, nil
	}
	ns, _, err := ts.resolver.LookupDefinition(ctx, definition)
	if err != nil {
		return nil, err
	}
	d, err := newDefinition(ts, ns)
	if err != nil {
		return nil, err
	}
	ts.definitions[definition] = d
	return d, nil
}
