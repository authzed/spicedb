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
	ns, err := ts.resolver.LookupDefinition(ctx, definition)
	if err != nil {
		return nil, err
	}
	d, err := newDefinition(ns)
	if err != nil {
		return nil, err
	}
	ts.definitions[definition] = d
	return d, nil
}

func (ts *TypeSystem) BuildReachabilityGraph(ctx context.Context) {

}
