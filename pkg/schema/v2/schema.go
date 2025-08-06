package schema

import "github.com/authzed/spicedb/pkg/schemadsl/compiler"

type Schema struct {
	Definitions map[string]*Definition
	Caveats     map[string]*Caveat
}

type Definition struct {
	parent      *Schema
	Name        string
	Relations   map[string]Relation
	Permissions map[string]Permission
}

type Caveat struct {
	parent         *Schema
	Name           string
	Expression     string
	ParameterTypes []string
}

type BaseRelation struct {
	Type        string
	Subrelation string
	Caveat      string
	Expiration  bool
	Wildcard    bool
}

type Relation struct {
	parent           *Definition
	Name             string
	BaseRelations    []BaseRelation
	AliasingRelation string
}

type Permission struct {
	parent    *Definition
	Name      string
	Operation Operation
}

func BuildSchemaFromCompiler(schema compiler.CompiledSchema) (*Schema, error) {
	out := &Schema{
		Definitions: make(map[string]*Definition),
		Caveats:     make(map[string]*Caveat),
	}

	for _, def := range schema.ObjectDefinitions {
		d, err := convertDefinition(def)
		d.parent = out
		if err != nil {
			return nil, err
		}
		out.Definitions[def.GetName()] = d
	}

	for _, caveat := range schema.CaveatDefinitions {
		c, err := convertCaveat(caveat)
		c.parent = out
		if err != nil {
			return nil, err
		}
		out.Caveats[caveat.GetName()] = c
	}

	return out, nil
}
