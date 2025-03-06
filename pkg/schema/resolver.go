package schema

import (
	"context"
	"fmt"
	"slices"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// Resolver is an interface defined for resolving referenced namespaces and caveats when constructing
// and validating a type system.
type Resolver interface {
	// LookupDefinition lookups up a namespace definition, also returning whether it was pre-validated.
	LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error)

	// LookupCaveat lookups up a caveat.
	LookupCaveat(ctx context.Context, name string) (*Caveat, error)
}

// ResolverForDatastoreReader returns a Resolver for a datastore reader.
func ResolverForDatastoreReader(ds datastore.Reader) *DatastoreResolver {
	return &DatastoreResolver{
		ds: ds,
	}
}

// PredefinedElements are predefined namespaces and/or caveats to give to a resolver.
type PredefinedElements struct {
	Definitions []*core.NamespaceDefinition
	Caveats     []*Caveat
}

func (pe PredefinedElements) combineWith(other PredefinedElements) PredefinedElements {
	return PredefinedElements{
		Definitions: append(slices.Clone(pe.Definitions), other.Definitions...),
		Caveats:     append(slices.Clone(pe.Caveats), other.Caveats...),
	}
}

// ResolverForPredefinedDefinitions returns a resolver for predefined namespaces and caveats.
func ResolverForPredefinedDefinitions(predefined PredefinedElements) Resolver {
	return &DatastoreResolver{
		predefined: predefined,
	}
}

// ResolverForSchema returns a resolver for a schema.
func ResolverForSchema(schema compiler.CompiledSchema) Resolver {
	return ResolverForPredefinedDefinitions(
		PredefinedElements{
			Definitions: schema.ObjectDefinitions,
			Caveats:     schema.CaveatDefinitions,
		},
	)
}

// DatastoreResolver is a resolver implementation for a datastore, to look up schema stored in the underlying storage.
type DatastoreResolver struct {
	ds         datastore.Reader
	predefined PredefinedElements
}

// LookupDefinition lookups up a namespace definition, also returning whether it was pre-validated.
func (r *DatastoreResolver) LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error) {
	if len(r.predefined.Definitions) > 0 {
		for _, def := range r.predefined.Definitions {
			if def.Name == name {
				return def, false, nil
			}
		}
	}

	if r.ds == nil {
		return nil, false, asTypeError(NewDefinitionNotFoundErr(name))
	}

	ns, _, err := r.ds.ReadNamespaceByName(ctx, name)
	return ns, true, err
}

// WithPredefinedElements adds elements (definitions and caveats) that will be used as a local overlay
// for the datastore, often for validation.
func (r *DatastoreResolver) WithPredefinedElements(predefined PredefinedElements) Resolver {
	return &DatastoreResolver{
		ds:         r.ds,
		predefined: predefined.combineWith(r.predefined),
	}
}

// LookupCaveat lookups up a caveat.
func (r *DatastoreResolver) LookupCaveat(ctx context.Context, name string) (*Caveat, error) {
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
