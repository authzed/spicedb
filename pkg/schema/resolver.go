package schema

import (
	"context"
	"slices"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// TypeSystemResolver provides an abstraction for fetching definitions and caveats from different sources (datastore, compiled schema, predefined elements).
type TypeSystemResolver interface {
	// LookupDefinition lookups up a namespace definition, also returning whether it was pre-validated.
	LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error)

	// LookupCaveat lookups up a caveat.
	LookupCaveat(ctx context.Context, name string) (*Caveat, error)
}

// ResolverFor returns a TypeSystemResolver for a SchemaReader.
func ResolverFor(sr datalayer.SchemaReader) *DatastoreResolver {
	return &DatastoreResolver{
		lookup: sr,
	}
}

// ResolverForSchemaReader returns a TypeSystemResolver for a datalayer reader.
// The reader's ReadSchema() method is called eagerly to resolve definitions.
func ResolverForSchemaReader(reader datalayer.RevisionedReader) (*DatastoreResolver, error) {
	sr, err := reader.ReadSchema()
	if err != nil {
		return nil, err
	}
	return &DatastoreResolver{lookup: sr}, nil
}

// ResolverForDatastoreReader returns a TypeSystemResolver for a datastore reader.
// This uses the Legacy* methods directly on the reader.
func ResolverForDatastoreReader(ds datastore.Reader) *DatastoreResolver {
	return &DatastoreResolver{
		lookup: datalayer.SchemaReaderFromLegacy(ds),
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
func ResolverForPredefinedDefinitions(predefined PredefinedElements) TypeSystemResolver {
	return &DatastoreResolver{
		predefined: predefined,
	}
}

// ResolverForSchema returns a resolver for a schema.
func ResolverForSchema(schema *compiler.CompiledSchema) TypeSystemResolver {
	return ResolverForPredefinedDefinitions(
		PredefinedElements{
			Definitions: schema.ObjectDefinitions,
			Caveats:     schema.CaveatDefinitions,
		},
	)
}

// DatastoreResolver is a resolver implementation for a datastore, to look up schema stored in the underlying storage.
type DatastoreResolver struct {
	lookup     datalayer.SchemaReader
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

	if r.lookup == nil {
		return nil, false, asTypeError(NewDefinitionNotFoundErr(name))
	}

	revDef, found, err := r.lookup.LookupTypeDefByName(ctx, name)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, asTypeError(NewDefinitionNotFoundErr(name))
	}
	ns := revDef.Definition
	return ns, true, nil
}

// WithPredefinedElements adds elements (definitions and caveats) that will be used as a local overlay
// for the datastore, often for validation.
func (r *DatastoreResolver) WithPredefinedElements(predefined PredefinedElements) TypeSystemResolver {
	return &DatastoreResolver{
		lookup:     r.lookup,
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

	if r.lookup == nil {
		return nil, asTypeError(NewCaveatNotFoundErr(name))
	}

	revDef, found, err := r.lookup.LookupCaveatDefByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, asTypeError(NewCaveatNotFoundErr(name))
	}
	caveatDef := revDef.Definition
	return caveatDef, nil
}
