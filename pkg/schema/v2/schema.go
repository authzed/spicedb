package schema

import (
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// schemaUnit is an interface for schema elements that can be cloned without a parent.
type schemaUnit[T any] interface {
	clone() T
}

// schemaUnitWithParent is an interface for schema elements that can be cloned with a parent.
type schemaUnitWithParent[T any, P any] interface {
	cloneWithParent(parent P) T
}

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

// GetTypeDefinition returns the type definition with the given name and a boolean
// indicating whether it exists in the schema.
func (s *Schema) GetTypeDefinition(name string) (*Definition, bool) {
	def, ok := s.definitions[name]
	return def, ok
}

// clone creates a deep copy of the Schema.
func (s *Schema) clone() *Schema {
	if s == nil {
		return nil
	}

	cloned := &Schema{
		definitions: make(map[string]*Definition, len(s.definitions)),
		caveats:     make(map[string]*Caveat, len(s.caveats)),
	}

	for name, def := range s.definitions {
		clonedDef := def.cloneWithParent(cloned)
		cloned.definitions[name] = clonedDef
	}

	for name, caveat := range s.caveats {
		clonedCaveat := caveat.cloneWithParent(cloned)
		cloned.caveats[name] = clonedCaveat
	}

	return cloned
}

var _ schemaUnit[*Schema] = &Schema{}

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

// GetRelation returns the relation with the given name and a boolean
// indicating whether it exists in the definition.
func (d *Definition) GetRelation(name string) (*Relation, bool) {
	rel, ok := d.relations[name]
	return rel, ok
}

// GetPermission returns the permission with the given name and a boolean
// indicating whether it exists in the definition.
func (d *Definition) GetPermission(name string) (*Permission, bool) {
	perm, ok := d.permissions[name]
	return perm, ok
}

// cloneWithParent creates a deep copy of the Definition with the specified parent.
func (d *Definition) cloneWithParent(parentSchema *Schema) *Definition {
	if d == nil {
		return nil
	}

	cloned := &Definition{
		parent:      parentSchema,
		name:        d.name,
		relations:   make(map[string]*Relation, len(d.relations)),
		permissions: make(map[string]*Permission, len(d.permissions)),
	}

	for name, rel := range d.relations {
		clonedRel := rel.cloneWithParent(cloned)
		cloned.relations[name] = clonedRel
	}

	for name, perm := range d.permissions {
		clonedPerm := perm.cloneWithParent(cloned)
		cloned.permissions[name] = clonedPerm
	}

	return cloned
}

var _ schemaUnitWithParent[*Definition, *Schema] = &Definition{}

// CaveatParameter represents a single parameter in a caveat with its name and type.
type CaveatParameter struct {
	name string
	typ  string
}

// Name returns the name of the parameter.
func (cp *CaveatParameter) Name() string {
	return cp.name
}

// Type returns the type of the parameter.
func (cp *CaveatParameter) Type() string {
	return cp.typ
}

// Caveat is a single, top-level caveat definition and it's internal expresion.
type Caveat struct {
	parent     *Schema
	name       string
	expression string
	parameters []CaveatParameter
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

// Parameters returns the parameters of the caveat.
func (c *Caveat) Parameters() []CaveatParameter {
	return c.parameters
}

// cloneWithParent creates a deep copy of the Caveat with the specified parent.
func (c *Caveat) cloneWithParent(parentSchema *Schema) *Caveat {
	if c == nil {
		return nil
	}

	parameters := make([]CaveatParameter, len(c.parameters))
	copy(parameters, c.parameters)

	return &Caveat{
		parent:     parentSchema,
		name:       c.name,
		expression: c.expression,
		parameters: parameters,
	}
}

var _ schemaUnitWithParent[*Caveat, *Schema] = &Caveat{}

type RelationOrPermission interface {
	isRelationOrPermission()
}

// Permission is a single `permission` line belonging to a definition. It has a name and a // and/or/not tree of operations representing it's right-hand-side.
type Permission struct {
	parent    *Definition
	name      string
	operation Operation
	synthetic bool // true if this permission was synthesized by the schema system
}

// SyntheticPermission is a permission that has been synthesized by the schema system
// (e.g., during flattening operations). It is functionally identical to a Permission
// but is marked as synthetic for tracking purposes.
type SyntheticPermission struct {
	Permission
	synthetic bool
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

// cloneWithParent creates a deep copy of the Permission with the specified parent.
func (p *Permission) cloneWithParent(parentDefinition *Definition) *Permission {
	if p == nil {
		return nil
	}

	return &Permission{
		parent:    parentDefinition,
		name:      p.name,
		operation: p.operation.clone(),
	}
}

// IsSynthetic returns true if this permission was synthesized by the schema system.
func (p *Permission) IsSynthetic() bool {
	return p.synthetic
}

// IsSynthetic returns true for synthetic permissions.
func (sp *SyntheticPermission) IsSynthetic() bool {
	return sp.synthetic
}

func (sp *SyntheticPermission) isRelationOrPermission() {}

var (
	_ RelationOrPermission                           = &Permission{}
	_ RelationOrPermission                           = &SyntheticPermission{}
	_ schemaUnitWithParent[*Permission, *Definition] = &Permission{}
)

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

// cloneWithParent creates a deep copy of the Relation with the specified parent.
func (r *Relation) cloneWithParent(parentDefinition *Definition) *Relation {
	if r == nil {
		return nil
	}

	cloned := &Relation{
		parent:           parentDefinition,
		name:             r.name,
		baseRelations:    make([]*BaseRelation, len(r.baseRelations)),
		aliasingRelation: r.aliasingRelation,
	}

	for i, br := range r.baseRelations {
		cloned.baseRelations[i] = br.cloneWithParent(cloned)
	}

	return cloned
}

var (
	_ RelationOrPermission                         = &Relation{}
	_ schemaUnitWithParent[*Relation, *Definition] = &Relation{}
)

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

// cloneWithParent creates a deep copy of the BaseRelation with the specified parent.
func (b *BaseRelation) cloneWithParent(parentRelation *Relation) *BaseRelation {
	if b == nil {
		return nil
	}

	return &BaseRelation{
		parent:      parentRelation,
		subjectType: b.subjectType,
		subrelation: b.subrelation,
		caveat:      b.caveat,
		expiration:  b.expiration,
		wildcard:    b.wildcard,
	}
}

var _ schemaUnitWithParent[*BaseRelation, *Relation] = &BaseRelation{}

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
		if err != nil {
			return nil, err
		}
		d.parent = out
		out.definitions[def.GetName()] = d
	}

	for _, caveat := range caveatDefs {
		c, err := convertCaveat(caveat)
		if err != nil {
			return nil, err
		}
		c.parent = out
		out.caveats[caveat.GetName()] = c
	}

	return out, nil
}
