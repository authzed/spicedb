package schema

import (
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// Schema is a view of a complete schema, with all definitions and caveats.
type Schema struct {
	Definitions map[string]*Definition
	Caveats     map[string]*Caveat
}

// Definition is a single schema object type, with relations and permissions.
type Definition struct {
	Parent      *Schema
	Name        string
	Relations   map[string]*Relation
	Permissions map[string]*Permission
}

// Caveat is a single, top-level caveat definition and it's internal expresion.
type Caveat struct {
	Parent         *Schema
	Name           string
	Expression     string
	ParameterTypes []string
}

// Permission is a single `permission` line belonging to a definition. It has a name and a // and/or/not tree of operations representing it's right-hand-side.
type Permission struct {
	Parent    *Definition
	Name      string
	Operation Operation
}

// Relation is a single `relation` line belonging to a definition. It has a name and list of types appearing on the right hand side.
type Relation struct {
	Parent           *Definition
	Name             string
	BaseRelations    []*BaseRelation
	AliasingRelation string
}

// BaseRelation is a single type, and its potential caveats, and expiration options. These features are written directly to the database with the parent Relation and Definition as the resource type and relation, and contains the subject type and optional subrelation.
type BaseRelation struct {
	Parent      *Relation
	Type        string
	Subrelation string
	Caveat      string
	Expiration  bool
	Wildcard    bool
}

// DefinitionName returns the name of the Definition in which this BaseRelation appears.
func (b BaseRelation) DefinitionName() string {
	return b.Parent.Parent.Name
}

// DefinitionName returns the name of the Relation in which this BaseRelation appears.
func (b BaseRelation) RelationName() string {
	return b.Parent.Name
}

// BuildSchemaFromCompiledSchema generates a Schema view from a CompiledSchema.
func BuildSchemaFromCompiledSchema(schema compiler.CompiledSchema) (*Schema, error) {
	return BuildSchemaFromDefinitions(schema.ObjectDefinitions, schema.CaveatDefinitions)
}

// BuildSchemaFromDefinitions generates a Schema view from the base core.v1 protos.
func BuildSchemaFromDefinitions(objectDefs []*corev1.NamespaceDefinition, caveatDefs []*corev1.CaveatDefinition) (*Schema, error) {
	out := &Schema{
		Definitions: make(map[string]*Definition),
		Caveats:     make(map[string]*Caveat),
	}

	for _, def := range objectDefs {
		d, err := convertDefinition(def)
		d.Parent = out
		if err != nil {
			return nil, err
		}
		out.Definitions[def.GetName()] = d
	}

	for _, caveat := range caveatDefs {
		c, err := convertCaveat(caveat)
		c.Parent = out
		if err != nil {
			return nil, err
		}
		out.Caveats[caveat.GetName()] = c
	}

	return out, nil
}
