package compiler

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
)

// CompiledSchemaResolver is a resolver for a fully compiled schema. It implements FullSchemaResolver,
// as it has the full context of the schema.
type CompiledSchemaResolver struct {
	schema *CompiledSchema
}

// ResolverForCompiledSchema builds a resolver from a compiled schema.
func ResolverForCompiledSchema(schema *CompiledSchema) *CompiledSchemaResolver {
	return &CompiledSchemaResolver{
		schema: schema,
	}
}

var _ schema.FullSchemaResolver = &CompiledSchemaResolver{}

// LookupDefinition lookups up a namespace, also returning whether it was pre-validated.
func (c CompiledSchemaResolver) LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error) {
	for _, o := range c.schema.ObjectDefinitions {
		if o.GetName() == name {
			return o, false, nil
		}
	}
	return nil, false, schema.NewDefinitionNotFoundErr(name)
}

// LookupCaveat lookups up a caveat.
func (c CompiledSchemaResolver) LookupCaveat(ctx context.Context, name string) (*schema.Caveat, error) {
	for _, v := range c.schema.CaveatDefinitions {
		if v.GetName() == name {
			return v, nil
		}
	}
	return nil, schema.NewCaveatNotFoundErr(name)
}

// AllDefinitionNames returns a list of all the names of defined namespaces for this resolved schema.
// Every definition is a valid parameter for LookupDefinition
func (c CompiledSchemaResolver) AllDefinitionNames() []string {
	out := make([]string, len(c.schema.ObjectDefinitions))
	for i, o := range c.schema.ObjectDefinitions {
		out[i] = o.GetName()
	}
	return out
}

// ResolverForSchema returns a resolver for a schema.
func ResolverForSchema(compiledSchema *CompiledSchema) schema.Resolver {
	return schema.ResolverForPredefinedDefinitions(
		schema.PredefinedElements{
			Definitions: compiledSchema.ObjectDefinitions,
			Caveats:     compiledSchema.CaveatDefinitions,
		},
	)
}
