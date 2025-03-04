package schema

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

type FullSchemaResolver interface {
	Resolver
	AllDefinitionNames() []string
}

type CompiledSchemaResolver struct {
	schema compiler.CompiledSchema
}

func ResolverForCompiledSchema(schema compiler.CompiledSchema) *CompiledSchemaResolver {
	return &CompiledSchemaResolver{
		schema: schema,
	}
}

var _ FullSchemaResolver = &CompiledSchemaResolver{}

// LookupNamespace lookups up a namespace, also returning whether it was pre-validated.
func (c CompiledSchemaResolver) LookupDefinition(ctx context.Context, name string) (*core.NamespaceDefinition, bool, error) {
	for _, o := range c.schema.ObjectDefinitions {
		if o.GetName() == name {
			return o, false, nil
		}
	}
	return nil, false, asTypeError(NewDefinitionNotFoundErr(name))
}

// LookupCaveat lookups up a caveat.
func (c CompiledSchemaResolver) LookupCaveat(ctx context.Context, name string) (*Caveat, error) {
	for _, v := range c.schema.CaveatDefinitions {
		if v.GetName() == name {
			return v, nil
		}
	}
	return nil, asTypeError(NewCaveatNotFoundErr(name))
}

func (c CompiledSchemaResolver) AllDefinitionNames() []string {
	out := make([]string, len(c.schema.ObjectDefinitions))
	for i, o := range c.schema.ObjectDefinitions {
		out[i] = o.GetName()
	}
	return out
}
