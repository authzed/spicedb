package namespace

import (
	"context"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// Resolver is an interface defined for resolving referenced namespaces and caveats when constructing
// and validating a type system.
type Resolver interface {
	// LookupNamespace lookups up a namespace.
	LookupNamespace(ctx context.Context, name string) (*core.NamespaceDefinition, error)

	// LookupCaveat lookups up a caveat.
	LookupCaveat(ctx context.Context, name string) (*core.CaveatDefinition, error)

	// WithPredefinedElements adds the given predefined elements to this resolver, returning a new
	// resolver.
	WithPredefinedElements(predefined PredefinedElements) Resolver
}

// ResolverForDatastoreReader returns a Resolver for a datastore reader.
func ResolverForDatastoreReader(ds datastore.Reader) Resolver {
	return &resolver{
		ds: ds,
	}
}

// PredefinedElements are predefined namespaces and/or caveats to give to a resolver.
type PredefinedElements struct {
	Namespaces []*core.NamespaceDefinition
	Caveats    []*core.CaveatDefinition
}

func (pe PredefinedElements) combineWith(other PredefinedElements) PredefinedElements {
	return PredefinedElements{
		Namespaces: append(slices.Clone(pe.Namespaces), other.Namespaces...),
		Caveats:    append(slices.Clone(pe.Caveats), other.Caveats...),
	}
}

// ResolverForPredefinedDefinitions returns a resolver for predefined namespaces and caveats.
func ResolverForPredefinedDefinitions(predefined PredefinedElements) Resolver {
	return &resolver{
		predefined: predefined,
	}
}

type resolver struct {
	ds         datastore.Reader
	predefined PredefinedElements
}

func (r *resolver) LookupNamespace(ctx context.Context, name string) (*core.NamespaceDefinition, error) {
	if len(r.predefined.Namespaces) > 0 {
		for _, def := range r.predefined.Namespaces {
			if def.Name == name {
				return def, nil
			}
		}
	}

	if r.ds == nil {
		return nil, asTypeError(NewNamespaceNotFoundErr(name))
	}

	ns, _, err := r.ds.ReadNamespaceByName(ctx, name)
	return ns, err
}

func (r *resolver) WithPredefinedElements(predefined PredefinedElements) Resolver {
	return &resolver{
		ds:         r.ds,
		predefined: predefined.combineWith(r.predefined),
	}
}

func (r *resolver) LookupCaveat(ctx context.Context, name string) (*core.CaveatDefinition, error) {
	if len(r.predefined.Caveats) > 0 {
		for _, caveat := range r.predefined.Caveats {
			if caveat.Name == name {
				return caveat, nil
			}
		}
	}

	if r.ds == nil {
		return nil, asTypeError(NewCaveatNotFoundErr(name))
	}

	cr, ok := r.ds.(datastore.CaveatReader)
	if !ok {
		return nil, fmt.Errorf("caveats are not supported on this datastore type")
	}

	caveatDef, _, err := cr.ReadCaveatByName(ctx, name)
	return caveatDef, err
}
