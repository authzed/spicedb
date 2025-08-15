package schema

import (
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

type Schema struct {
	Definitions map[string]*Definition
	Caveats     map[string]*Caveat
}

type Definition struct {
	Parent      *Schema
	Name        string
	Relations   map[string]*Relation
	Permissions map[string]*Permission
}

type Caveat struct {
	Parent         *Schema
	Name           string
	Expression     string
	ParameterTypes []string
}

type BaseRelation struct {
	Parent      *Relation
	Type        string
	Subrelation string
	Caveat      string
	Expiration  bool
	Wildcard    bool
}

func (b BaseRelation) DefinitionName() string {
	return b.Parent.Parent.Name
}

func (b BaseRelation) RelationName() string {
	return b.Parent.Name
}

type Relation struct {
	Parent           *Definition
	Name             string
	BaseRelations    []*BaseRelation
	AliasingRelation string
}

type Permission struct {
	Parent    *Definition
	Name      string
	Operation Operation
}

func BuildSchemaFromCompiler(schema compiler.CompiledSchema) (*Schema, error) {
	return BuildSchemaFromDefinitions(schema.ObjectDefinitions, schema.CaveatDefinitions)
}

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
