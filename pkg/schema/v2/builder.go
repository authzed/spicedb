package schema

import "github.com/authzed/spicedb/pkg/tuple"

// Common caveat parameter type constants for use with CaveatBuilder.
const (
	CaveatTypeInt        = "int"
	CaveatTypeBool       = "bool"
	CaveatTypeString     = "string"
	CaveatTypeBytes      = "bytes"
	CaveatTypeDouble     = "double"
	CaveatTypeUInt       = "uint"
	CaveatTypeDuration   = "duration"
	CaveatTypeTimestamp  = "timestamp"
	CaveatTypeIPAddress  = "ipaddress"
	CaveatTypeList       = "list"
	CaveatTypeMap        = "map"
	CaveatTypeAny        = "any"
	CaveatTypeListString = "list<string>"
	CaveatTypeListInt    = "list<int>"
	CaveatTypeMapAny     = "map<any>"
)

// SchemaBuilder provides a fluent API for constructing schemas programmatically.
// It maintains parent-child relationships automatically and provides method chaining.
//
// Example usage:
//
//	schema := NewSchemaBuilder().
//		AddDefinition("user").
//			Done().
//		AddDefinition("document").
//			AddRelation("owner").
//				AllowedDirectRelation("user").
//				Done().
//			AddRelation("viewer").
//				AllowedDirectRelation("user").
//				AllowedRelation("group", "member").
//				Done().
//			AddPermission("view").
//				UnionExpr().
//					Add(RelRef("owner")).
//					Add(RelRef("viewer")).
//					Done().
//				Done().
//			Done().
//		AddCaveat("has_access").
//			Expression("resource == 42").
//			Parameter("resource", CaveatTypeInt).
//			Done().
//		Build()
type SchemaBuilder struct {
	schema *Schema
}

// NewSchemaBuilder creates a new builder for constructing schemas.
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		schema: &Schema{
			definitions: make(map[string]*Definition),
			caveats:     make(map[string]*Caveat),
		},
	}
}

// AddDefinition adds or modifies a definition in the schema by name.
// Returns a DefinitionBuilder for fluent configuration.
func (sb *SchemaBuilder) AddDefinition(name string) *DefinitionBuilder {
	def, exists := sb.schema.definitions[name]
	if !exists {
		def = &Definition{
			parent:      sb.schema,
			name:        name,
			relations:   make(map[string]*Relation),
			permissions: make(map[string]*Permission),
		}
		sb.schema.definitions[name] = def
	}
	return &DefinitionBuilder{
		schemaBuilder: sb,
		definition:    def,
	}
}

// Definition accepts a DefinitionBuilder and integrates it into the schema.
// Returns the SchemaBuilder for continued chaining.
func (sb *SchemaBuilder) Definition(db *DefinitionBuilder) *SchemaBuilder {
	if db != nil && db.definition != nil {
		// Update the parent reference
		db.definition.parent = sb.schema
		db.schemaBuilder = sb
		// Add or update the definition in the schema
		sb.schema.definitions[db.definition.name] = db.definition
	}
	return sb
}

// AddCaveat adds or modifies a caveat in the schema by name.
// Returns a CaveatBuilder for fluent configuration.
func (sb *SchemaBuilder) AddCaveat(name string) *CaveatBuilder {
	caveat, exists := sb.schema.caveats[name]
	if !exists {
		caveat = &Caveat{
			parent:     sb.schema,
			name:       name,
			parameters: []CaveatParameter{},
		}
		sb.schema.caveats[name] = caveat
	}
	return &CaveatBuilder{
		schemaBuilder: sb,
		caveat:        caveat,
	}
}

// Caveat accepts a CaveatBuilder and integrates it into the schema.
// Returns the SchemaBuilder for continued chaining.
func (sb *SchemaBuilder) Caveat(cb *CaveatBuilder) *SchemaBuilder {
	if cb != nil && cb.caveat != nil {
		// Update the parent reference
		cb.caveat.parent = sb.schema
		cb.schemaBuilder = sb
		// Add or update the caveat in the schema
		sb.schema.caveats[cb.caveat.name] = cb.caveat
	}
	return sb
}

// Build finalizes and returns the constructed schema.
func (sb *SchemaBuilder) Build() *Schema {
	return sb.schema
}

// DefinitionBuilder provides a fluent API for constructing object definitions.
type DefinitionBuilder struct {
	schemaBuilder *SchemaBuilder
	definition    *Definition
}

// AddRelation adds or modifies a relation in the definition by name.
// Returns a RelationBuilder for fluent configuration.
func (db *DefinitionBuilder) AddRelation(name string) *RelationBuilder {
	relation, exists := db.definition.relations[name]
	if !exists {
		relation = &Relation{
			parent:        db.definition,
			name:          name,
			baseRelations: []*BaseRelation{},
		}
		db.definition.relations[name] = relation
	}
	return &RelationBuilder{
		definitionBuilder: db,
		relation:          relation,
	}
}

// Relation accepts a RelationBuilder and integrates it into the definition.
// Returns the DefinitionBuilder for continued chaining.
func (db *DefinitionBuilder) Relation(rb *RelationBuilder) *DefinitionBuilder {
	if rb != nil && rb.relation != nil {
		// Update the parent reference
		rb.relation.parent = db.definition
		rb.definitionBuilder = db
		// Add or update the relation in the definition
		db.definition.relations[rb.relation.name] = rb.relation
	}
	return db
}

// AddPermission adds or modifies a permission in the definition by name.
// Returns a PermissionBuilder for fluent configuration.
func (db *DefinitionBuilder) AddPermission(name string) *PermissionBuilder {
	permission, exists := db.definition.permissions[name]
	if !exists {
		permission = &Permission{
			parent:    db.definition,
			name:      name,
			synthetic: false,
		}
		db.definition.permissions[name] = permission
	}
	return &PermissionBuilder{
		definitionBuilder: db,
		permission:        permission,
	}
}

// Permission accepts a PermissionBuilder and integrates it into the definition.
// Returns the DefinitionBuilder for continued chaining.
func (db *DefinitionBuilder) Permission(pb *PermissionBuilder) *DefinitionBuilder {
	if pb != nil && pb.permission != nil {
		// Update the parent reference
		pb.permission.parent = db.definition
		pb.definitionBuilder = db
		// Add or update the permission in the definition
		db.definition.permissions[pb.permission.name] = pb.permission
	}
	return db
}

// Done completes the definition and returns to the schema builder.
func (db *DefinitionBuilder) Done() *SchemaBuilder {
	return db.schemaBuilder
}

// RelationBuilder provides a fluent API for constructing relations.
type RelationBuilder struct {
	definitionBuilder *DefinitionBuilder
	relation          *Relation
}

// AllowedDirectRelation adds a direct subject type to the relation.
// This allows subjects of the specified type to be directly assigned to this relation.
func (rb *RelationBuilder) AllowedDirectRelation(subjectType string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: tuple.Ellipsis,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedRelation adds a base relation (allowed subject type with subrelation) to the relation.
// subrelation specifies a specific relation/permission on the subject type.
func (rb *RelationBuilder) AllowedRelation(subjectType, subrelation string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: subrelation,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedDirectRelationWithCaveat adds a direct subject type with a caveat.
func (rb *RelationBuilder) AllowedDirectRelationWithCaveat(subjectType, caveat string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: tuple.Ellipsis,
		caveat:      caveat,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedRelationWithCaveat adds a base relation with a caveat.
func (rb *RelationBuilder) AllowedRelationWithCaveat(subjectType, subrelation, caveat string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: subrelation,
		caveat:      caveat,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedDirectRelationWithExpiration adds a direct subject type with expiration support.
func (rb *RelationBuilder) AllowedDirectRelationWithExpiration(subjectType string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: tuple.Ellipsis,
		expiration:  true,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedRelationWithExpiration adds a base relation with expiration support.
func (rb *RelationBuilder) AllowedRelationWithExpiration(subjectType, subrelation string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: subrelation,
		expiration:  true,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedDirectRelationWithFeatures adds a direct subject type with full feature control.
func (rb *RelationBuilder) AllowedDirectRelationWithFeatures(subjectType, caveat string, expiration bool) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: tuple.Ellipsis,
		caveat:      caveat,
		expiration:  expiration,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedRelationWithFeatures adds a base relation with full feature control.
func (rb *RelationBuilder) AllowedRelationWithFeatures(subjectType, subrelation, caveat string, expiration bool) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		subrelation: subrelation,
		caveat:      caveat,
		expiration:  expiration,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedWildcard adds a wildcard base relation.
func (rb *RelationBuilder) AllowedWildcard(subjectType string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		wildcard:    true,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// AllowedWildcardWithCaveat adds a wildcard base relation with a caveat.
func (rb *RelationBuilder) AllowedWildcardWithCaveat(subjectType, caveat string) *RelationBuilder {
	baseRel := &BaseRelation{
		parent:      rb.relation,
		subjectType: subjectType,
		wildcard:    true,
		caveat:      caveat,
	}
	rb.relation.baseRelations = append(rb.relation.baseRelations, baseRel)
	return rb
}

// Alias sets this relation as an alias to another relation.
func (rb *RelationBuilder) Alias(aliasingRelation string) *RelationBuilder {
	rb.relation.aliasingRelation = aliasingRelation
	return rb
}

// Done completes the relation and returns to the definition builder.
func (rb *RelationBuilder) Done() *DefinitionBuilder {
	return rb.definitionBuilder
}

// PermissionBuilder provides a fluent API for constructing permissions.
type PermissionBuilder struct {
	definitionBuilder *DefinitionBuilder
	permission        *Permission
}

// Operation sets the permission's operation directly.
func (pb *PermissionBuilder) Operation(op Operation) *PermissionBuilder {
	pb.permission.operation = op
	return pb
}

// RelationRef creates a simple relation reference operation (e.g., "viewer").
func (pb *PermissionBuilder) RelationRef(relationName string) *PermissionBuilder {
	pb.permission.operation = &RelationReference{
		relationName: relationName,
	}
	return pb
}

// Arrow creates an arrow reference operation (e.g., "owner->edit").
func (pb *PermissionBuilder) Arrow(relationName, permissionName string) *PermissionBuilder {
	pb.permission.operation = &ArrowReference{
		left:  relationName,
		right: permissionName,
	}
	return pb
}

// IntersectionArrow creates an intersection arrow operation (e.g., "relation.all(permission)").
func (pb *PermissionBuilder) IntersectionArrow(relation string, computedRelationName string) *PermissionBuilder {
	pb.permission.operation = &FunctionedArrowReference{
		left:     relation,
		right:    computedRelationName,
		function: FunctionTypeAll,
	}
	return pb
}

// Union creates a union (OR) operation from multiple operations.
func (pb *PermissionBuilder) Union(operations ...Operation) *PermissionBuilder {
	pb.permission.operation = &UnionOperation{
		children: operations,
	}
	return pb
}

// UnionExpr starts building a union expression incrementally.
func (pb *PermissionBuilder) UnionExpr() *UnionExprBuilder {
	return &UnionExprBuilder{
		permissionBuilder: pb,
		children:          []Operation{},
	}
}

// Intersection creates an intersection (AND) operation from multiple operations.
func (pb *PermissionBuilder) Intersection(operations ...Operation) *PermissionBuilder {
	pb.permission.operation = &IntersectionOperation{
		children: operations,
	}
	return pb
}

// IntersectionExpr starts building an intersection expression incrementally.
func (pb *PermissionBuilder) IntersectionExpr() *IntersectionExprBuilder {
	return &IntersectionExprBuilder{
		permissionBuilder: pb,
		children:          []Operation{},
	}
}

// Exclusion creates an exclusion (subtraction) operation.
func (pb *PermissionBuilder) Exclusion(base, excluded Operation) *PermissionBuilder {
	pb.permission.operation = &ExclusionOperation{
		left:  base,
		right: excluded,
	}
	return pb
}

// ExclusionExpr starts building an exclusion expression incrementally.
func (pb *PermissionBuilder) ExclusionExpr() *ExclusionExprBuilder {
	return &ExclusionExprBuilder{
		permissionBuilder: pb,
	}
}

// Done completes the permission and returns to the definition builder.
func (pb *PermissionBuilder) Done() *DefinitionBuilder {
	return pb.definitionBuilder
}

// UnionExprBuilder allows building union operations incrementally.
type UnionExprBuilder struct {
	permissionBuilder *PermissionBuilder
	children          []Operation
}

// Add adds an operation to the union.
func (ub *UnionExprBuilder) Add(op Operation) *UnionExprBuilder {
	ub.children = append(ub.children, op)
	return ub
}

// AddRelationRef adds a relation reference to the union.
func (ub *UnionExprBuilder) AddRelationRef(relationName string) *UnionExprBuilder {
	ub.children = append(ub.children, &RelationReference{
		relationName: relationName,
	})
	return ub
}

// AddArrow adds an arrow reference to the union.
func (ub *UnionExprBuilder) AddArrow(relationName, permissionName string) *UnionExprBuilder {
	ub.children = append(ub.children, &ArrowReference{
		left:  relationName,
		right: permissionName,
	})
	return ub
}

// Done completes the union expression and returns to the permission builder.
func (ub *UnionExprBuilder) Done() *PermissionBuilder {
	ub.permissionBuilder.permission.operation = &UnionOperation{
		children: ub.children,
	}
	return ub.permissionBuilder
}

// Build completes the union expression and returns it as an Operation.
// This is used when building operations outside of a permission builder context.
func (ub *UnionExprBuilder) Build() Operation {
	return &UnionOperation{
		children: ub.children,
	}
}

// IntersectionExprBuilder allows building intersection operations incrementally.
type IntersectionExprBuilder struct {
	permissionBuilder *PermissionBuilder
	children          []Operation
}

// Add adds an operation to the intersection.
func (ib *IntersectionExprBuilder) Add(op Operation) *IntersectionExprBuilder {
	ib.children = append(ib.children, op)
	return ib
}

// AddRelationRef adds a relation reference to the intersection.
func (ib *IntersectionExprBuilder) AddRelationRef(relationName string) *IntersectionExprBuilder {
	ib.children = append(ib.children, &RelationReference{
		relationName: relationName,
	})
	return ib
}

// AddArrow adds an arrow reference to the intersection.
func (ib *IntersectionExprBuilder) AddArrow(relationName, permissionName string) *IntersectionExprBuilder {
	ib.children = append(ib.children, &ArrowReference{
		left:  relationName,
		right: permissionName,
	})
	return ib
}

// Done completes the intersection expression and returns to the permission builder.
func (ib *IntersectionExprBuilder) Done() *PermissionBuilder {
	ib.permissionBuilder.permission.operation = &IntersectionOperation{
		children: ib.children,
	}
	return ib.permissionBuilder
}

// Build completes the intersection expression and returns it as an Operation.
// This is used when building operations outside of a permission builder context.
func (ib *IntersectionExprBuilder) Build() Operation {
	return &IntersectionOperation{
		children: ib.children,
	}
}

// ExclusionExprBuilder allows building exclusion operations incrementally.
type ExclusionExprBuilder struct {
	permissionBuilder *PermissionBuilder
	base              Operation
	excluded          Operation
}

// Base sets the base operation for the exclusion.
func (eb *ExclusionExprBuilder) Base(op Operation) *ExclusionExprBuilder {
	eb.base = op
	return eb
}

// BaseRelationRef sets the base as a relation reference.
func (eb *ExclusionExprBuilder) BaseRelationRef(relationName string) *ExclusionExprBuilder {
	eb.base = &RelationReference{
		relationName: relationName,
	}
	return eb
}

// BaseArrow sets the base as an arrow reference.
func (eb *ExclusionExprBuilder) BaseArrow(relationName, permissionName string) *ExclusionExprBuilder {
	eb.base = &ArrowReference{
		left:  relationName,
		right: permissionName,
	}
	return eb
}

// Exclude sets the operation to exclude from the base.
func (eb *ExclusionExprBuilder) Exclude(op Operation) *ExclusionExprBuilder {
	eb.excluded = op
	return eb
}

// ExcludeRelationRef sets the excluded operation as a relation reference.
func (eb *ExclusionExprBuilder) ExcludeRelationRef(relationName string) *ExclusionExprBuilder {
	eb.excluded = &RelationReference{
		relationName: relationName,
	}
	return eb
}

// ExcludeArrow sets the excluded operation as an arrow reference.
func (eb *ExclusionExprBuilder) ExcludeArrow(relationName, permissionName string) *ExclusionExprBuilder {
	eb.excluded = &ArrowReference{
		left:  relationName,
		right: permissionName,
	}
	return eb
}

// Done completes the exclusion expression and returns to the permission builder.
func (eb *ExclusionExprBuilder) Done() *PermissionBuilder {
	eb.permissionBuilder.permission.operation = &ExclusionOperation{
		left:  eb.base,
		right: eb.excluded,
	}
	return eb.permissionBuilder
}

// Build completes the exclusion expression and returns it as an Operation.
// This is used when building operations outside of a permission builder context.
func (eb *ExclusionExprBuilder) Build() Operation {
	return &ExclusionOperation{
		left:  eb.base,
		right: eb.excluded,
	}
}

// CaveatBuilder provides a fluent API for constructing caveats.
type CaveatBuilder struct {
	schemaBuilder *SchemaBuilder
	caveat        *Caveat
}

// Expression sets the caveat's expression.
func (cb *CaveatBuilder) Expression(expr string) *CaveatBuilder {
	cb.caveat.expression = expr
	return cb
}

// Parameter adds a parameter with name and type to the caveat.
func (cb *CaveatBuilder) Parameter(paramName, paramType string) *CaveatBuilder {
	cb.caveat.parameters = append(cb.caveat.parameters, CaveatParameter{
		name: paramName,
		typ:  paramType,
	})
	return cb
}

// ParameterMap adds multiple parameters from a map of name to type.
// This is useful when you have many parameters to add at once.
func (cb *CaveatBuilder) ParameterMap(params map[string]string) *CaveatBuilder {
	for name, typ := range params {
		cb.caveat.parameters = append(cb.caveat.parameters, CaveatParameter{
			name: name,
			typ:  typ,
		})
	}
	return cb
}

// Done completes the caveat and returns to the schema builder.
func (cb *CaveatBuilder) Done() *SchemaBuilder {
	return cb.schemaBuilder
}

// Helper functions for creating operations inline

// RelRef creates a RelationReference operation for use in permission builders.
func RelRef(relationName string) Operation {
	return &RelationReference{
		relationName: relationName,
	}
}

// ArrowRef creates an ArrowReference operation for use in permission builders.
func ArrowRef(relationName, permissionName string) Operation {
	return &ArrowReference{
		left:  relationName,
		right: permissionName,
	}
}

// IntersectionArrowRef creates an intersection arrow operation for use in permission builders.
func IntersectionArrowRef(relation string, computedRelationName string) Operation {
	return &FunctionedArrowReference{
		left:     relation,
		right:    computedRelationName,
		function: FunctionTypeAll,
	}
}

// Union creates a UnionOperation for use in permission builders.
func Union(operations ...Operation) Operation {
	return &UnionOperation{
		children: operations,
	}
}

// Intersection creates an IntersectionOperation for use in permission builders.
func Intersection(operations ...Operation) Operation {
	return &IntersectionOperation{
		children: operations,
	}
}

// Exclusion creates an ExclusionOperation for use in permission builders.
func Exclusion(base, excluded Operation) Operation {
	return &ExclusionOperation{
		left:  base,
		right: excluded,
	}
}

// Standalone constructor functions for building schema elements

// NewDefinition creates a new DefinitionBuilder for constructing a definition.
// This is used with SchemaBuilder.Definition() to add definitions using the builder pattern.
func NewDefinition(name string) *DefinitionBuilder {
	def := &Definition{
		name:        name,
		relations:   make(map[string]*Relation),
		permissions: make(map[string]*Permission),
	}
	return &DefinitionBuilder{
		definition: def,
	}
}

// NewRelation creates a new RelationBuilder for constructing a relation.
// This is used with DefinitionBuilder.Relation() to add relations using the builder pattern.
func NewRelation(name string) *RelationBuilder {
	rel := &Relation{
		name:          name,
		baseRelations: []*BaseRelation{},
	}
	return &RelationBuilder{
		relation: rel,
	}
}

// NewPermission creates a new PermissionBuilder for constructing a permission with an operation.
// This is used with DefinitionBuilder.Permission() to add permissions using the builder pattern.
func NewPermission(name string, operation Operation) *PermissionBuilder {
	perm := &Permission{
		name:      name,
		operation: operation,
		synthetic: false,
	}
	return &PermissionBuilder{
		permission: perm,
	}
}

// NewCaveat creates a new CaveatBuilder for constructing a caveat.
// This is used with SchemaBuilder.Caveat() to add caveats using the builder pattern.
func NewCaveat(name string) *CaveatBuilder {
	cav := &Caveat{
		name:       name,
		parameters: []CaveatParameter{},
	}
	return &CaveatBuilder{
		caveat: cav,
	}
}

// NewUnion creates a new UnionExprBuilder for constructing a union operation.
// This is useful when you want to build a union expression outside of a permission builder context.
func NewUnion() *UnionExprBuilder {
	return &UnionExprBuilder{
		children: []Operation{},
	}
}

// NewIntersection creates a new IntersectionExprBuilder for constructing an intersection operation.
// This is useful when you want to build an intersection expression outside of a permission builder context.
func NewIntersection() *IntersectionExprBuilder {
	return &IntersectionExprBuilder{
		children: []Operation{},
	}
}

// NewExclusion creates a new ExclusionExprBuilder for constructing an exclusion operation.
// This is useful when you want to build an exclusion expression outside of a permission builder context.
func NewExclusion() *ExclusionExprBuilder {
	return &ExclusionExprBuilder{}
}

// NewRelationRef creates a RelationReference operation for the given relation name.
// This is an alternative to RelRef() that follows the New* naming convention.
func NewRelationRef(relationName string) Operation {
	return &RelationReference{
		relationName: relationName,
	}
}

// NewArrow creates an ArrowReference operation for the given relation and permission names.
// This is an alternative to ArrowRef() that follows the New* naming convention.
func NewArrow(relationName, permissionName string) Operation {
	return &ArrowReference{
		left:  relationName,
		right: permissionName,
	}
}
