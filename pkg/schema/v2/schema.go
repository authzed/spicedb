package schema

import (
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// Schema is a view of a complete schema, with all definitions and caveats.
type Schema struct {
	definitions map[string]*Definition
	caveats     map[string]*Caveat
}

// Definitions returns the definitions in the schema.
func (s *Schema) Definitions() map[string]*Definition {
	return s.definitions
}

// Caveats returns the caveats in the schema.
func (s *Schema) Caveats() map[string]*Caveat {
	return s.caveats
}

// Definition is a single schema object type, with relations and permissions.
type Definition struct {
	parent      *Schema
	name        string
	relations   map[string]*Relation
	permissions map[string]*Permission
}

// Parent returns the parent schema.
func (d *Definition) Parent() *Schema {
	return d.parent
}

// Name returns the name of the definition.
func (d *Definition) Name() string {
	return d.name
}

// Relations returns the relations in the definition.
func (d *Definition) Relations() map[string]*Relation {
	return d.relations
}

// Permissions returns the permissions in the definition.
func (d *Definition) Permissions() map[string]*Permission {
	return d.permissions
}

// Caveat is a single, top-level caveat definition and it's internal expresion.
type Caveat struct {
	parent         *Schema
	name           string
	expression     string
	parameterTypes []string
}

// Parent returns the parent schema.
func (c *Caveat) Parent() *Schema {
	return c.parent
}

// Name returns the name of the caveat.
func (c *Caveat) Name() string {
	return c.name
}

// Expression returns the expression of the caveat.
func (c *Caveat) Expression() string {
	return c.expression
}

// ParameterTypes returns the parameter types of the caveat.
func (c *Caveat) ParameterTypes() []string {
	return c.parameterTypes
}

type RelationOrPermission interface {
	isRelationOrPermission()
}

// Permission is a single `permission` line belonging to a definition. It has a name and a // and/or/not tree of operations representing it's right-hand-side.
type Permission struct {
	parent    *Definition
	name      string
	operation Operation
}

// Parent returns the parent definition.
func (p *Permission) Parent() *Definition {
	return p.parent
}

// Name returns the name of the permission.
func (p *Permission) Name() string {
	return p.name
}

// Operation returns the operation of the permission.
func (p *Permission) Operation() Operation {
	return p.operation
}

func (p *Permission) isRelationOrPermission() {}

var _ RelationOrPermission = &Permission{}

// Relation is a single `relation` line belonging to a definition. It has a name and list of types appearing on the right hand side.
type Relation struct {
	parent           *Definition
	name             string
	baseRelations    []*BaseRelation
	aliasingRelation string
}

// Parent returns the parent definition.
func (r *Relation) Parent() *Definition {
	return r.parent
}

// Name returns the name of the relation.
func (r *Relation) Name() string {
	return r.name
}

// BaseRelations returns the base relations of the relation.
func (r *Relation) BaseRelations() []*BaseRelation {
	return r.baseRelations
}

// AliasingRelation returns the aliasing relation of the relation.
func (r *Relation) AliasingRelation() string {
	return r.aliasingRelation
}

func (r *Relation) isRelationOrPermission() {}

var _ RelationOrPermission = &Relation{}

// BaseRelation is a single type, and its potential caveats, and expiration options. These features are written directly to the database with the parent Relation and Definition as the resource type and relation, and contains the subject type and optional subrelation.
type BaseRelation struct {
	parent      *Relation
	subjectType string
	subrelation string
	caveat      string
	expiration  bool
	wildcard    bool
}

// Parent returns the parent relation.
func (b *BaseRelation) Parent() *Relation {
	return b.parent
}

// Type returns the subject type of the base relation.
func (b *BaseRelation) Type() string {
	return b.subjectType
}

// Subrelation returns the subrelation of the base relation.
func (b *BaseRelation) Subrelation() string {
	return b.subrelation
}

// Caveat returns the caveat of the base relation.
func (b *BaseRelation) Caveat() string {
	return b.caveat
}

// Expiration returns whether the base relation has expiration.
func (b *BaseRelation) Expiration() bool {
	return b.expiration
}

// Wildcard returns whether the base relation is a wildcard.
func (b *BaseRelation) Wildcard() bool {
	return b.wildcard
}

// DefinitionName returns the name of the Definition in which this BaseRelation appears.
func (b *BaseRelation) DefinitionName() string {
	return b.parent.parent.name
}

// RelationName returns the name of the Relation in which this BaseRelation appears.
func (b *BaseRelation) RelationName() string {
	return b.parent.name
}

// BuildSchemaFromCompiledSchema generates a Schema view from a CompiledSchema.
func BuildSchemaFromCompiledSchema(schema compiler.CompiledSchema) (*Schema, error) {
	return BuildSchemaFromDefinitions(schema.ObjectDefinitions, schema.CaveatDefinitions)
}

// BuildSchemaFromDefinitions generates a Schema view from the base core.v1 protos.
func BuildSchemaFromDefinitions(objectDefs []*corev1.NamespaceDefinition, caveatDefs []*corev1.CaveatDefinition) (*Schema, error) {
	out := &Schema{
		definitions: make(map[string]*Definition),
		caveats:     make(map[string]*Caveat),
	}

	for _, def := range objectDefs {
		d, err := convertDefinition(def)
		d.parent = out
		if err != nil {
			return nil, err
		}
		out.definitions[def.GetName()] = d
	}

	for _, caveat := range caveatDefs {
		c, err := convertCaveat(caveat)
		c.parent = out
		if err != nil {
			return nil, err
		}
		out.caveats[caveat.GetName()] = c
	}

	return out, nil
}
