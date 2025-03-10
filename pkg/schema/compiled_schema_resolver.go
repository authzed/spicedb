package schema

import (
	"context"

	"github.com/authzed/spicedb/pkg/commonschemadsl"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// FullSchemaResolver is a superset of a resolver that knows how to retrieve all definitions
// from its source by name (by having a complete list of names).
type FullSchemaResolver interface {
	Resolver
	AllDefinitionNames() []string
}

// CompiledSchemaResolver is a resolver for a fully compiled schema. It implements FullSchemaResolver,
// as it has the full context of the schema.
type CompiledSchemaResolver struct {
	schema commonschemadsl.CompiledSchema
}

// ResolverForCompiledSchema builds a resolver from a compiled schema.
func ResolverForCompiledSchema(schema commonschemadsl.CompiledSchema) *CompiledSchemaResolver {
	return &CompiledSchemaResolver{
		schema: schema,
	}
}

var _ FullSchemaResolver = &CompiledSchemaResolver{}

// LookupDefinition lookups up a namespace, also returning whether it was pre-validated.
func (c CompiledSchemaResolver) LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error) {
	for _, o := range c.schema.GetObjectDefinitions() {
		if o.GetName() == name {
			return o, false, nil
		}
	}
	return nil, false, asTypeError(NewDefinitionNotFoundErr(name))
}

// LookupCaveat lookups up a caveat.
func (c CompiledSchemaResolver) LookupCaveat(ctx context.Context, name string) (*Caveat, error) {
	for _, v := range c.schema.GetCaveatDefinitions() {
		if v.GetName() == name {
			return v, nil
		}
	}
	return nil, asTypeError(NewCaveatNotFoundErr(name))
}

// AllDefinitionNames returns a list of all the names of defined namespaces for this resolved schema.
// Every definition is a valid parameter for LookupDefinition
func (c CompiledSchemaResolver) AllDefinitionNames() []string {
	out := make([]string, len(c.schema.GetObjectDefinitions()))
	for i, o := range c.schema.GetObjectDefinitions() {
		out[i] = o.GetName()
	}
	return out
}
