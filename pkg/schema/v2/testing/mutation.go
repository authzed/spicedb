package testing

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	schemapkg "github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// MutationKind is the kind of change a Mutation makes to a schema.
type MutationKind int

const (
	// AddDefinition adds a new definition, along with its relations and permissions.
	AddDefinition MutationKind = iota

	// RemoveDefinition removes a definition.
	RemoveDefinition

	// AddRelation adds a new relation to a definition.
	AddRelation

	// RemoveRelation removes a relation from a definition.
	RemoveRelation

	// AddAllowedType adds an allowed subject type to a relation.
	AddAllowedType

	// RemoveAllowedType removes an allowed subject type from a relation.
	RemoveAllowedType

	// AddPermission adds a new permission to a definition.
	AddPermission

	// RemovePermission removes a permission from a definition.
	RemovePermission

	// ChangePermission replaces the expression of a permission.
	ChangePermission

	// AddUnionChild extends the expression of a permission, by adding a child to the union
	// at the root of the expression. If the expression is not a union, it becomes a union
	// of the existing expression and the child.
	AddUnionChild

	// RemoveUnionChild narrows the expression of a permission, by removing a child from the
	// union at the root of the expression.
	RemoveUnionChild
)

var mutationKindNames = map[MutationKind]string{
	AddDefinition:     "add-definition",
	RemoveDefinition:  "remove-definition",
	AddRelation:       "add-relation",
	RemoveRelation:    "remove-relation",
	AddAllowedType:    "add-allowed-type",
	RemoveAllowedType: "remove-allowed-type",
	AddPermission:     "add-permission",
	RemovePermission:  "remove-permission",
	ChangePermission:  "change-permission",
	AddUnionChild:     "add-union-child",
	RemoveUnionChild:  "remove-union-child",
}

func (k MutationKind) String() string {
	if name, ok := mutationKindNames[k]; ok {
		return name
	}
	return fmt.Sprintf("unknown-mutation-kind(%d)", int(k))
}

// AllowedType is a subject type allowed on a relation. Subrelation is tuple.Ellipsis
// for a direct subject.
type AllowedType struct {
	Type        string
	Subrelation string
}

func (at AllowedType) String() string {
	if at.Subrelation == tuple.Ellipsis {
		return at.Type
	}
	return at.Type + "#" + at.Subrelation
}

// Mutation is a single change to a schema.
type Mutation struct {
	// Kind is the kind of change being made.
	Kind MutationKind

	// Definition is the name of the definition being changed.
	Definition string

	// Name is the name of the relation or permission being changed. Empty for
	// AddDefinition and RemoveDefinition.
	Name string

	// AllowedTypes holds all of the allowed subject types for AddRelation, and exactly
	// one for AddAllowedType and RemoveAllowedType.
	AllowedTypes []AllowedType

	// Operation is the expression for AddPermission and ChangePermission, and the child
	// being added or removed for AddUnionChild and RemoveUnionChild. If a union has
	// multiple children matching that of a RemoveUnionChild, the last is removed.
	Operation schema.Operation

	// Members are the AddRelation and AddPermission mutations making up an AddDefinition.
	Members []Mutation
}

// BreaksData returns whether the mutation can leave existing relationships without
// a place in the schema.
func (m Mutation) BreaksData() bool {
	switch m.Kind {
	case RemoveDefinition, RemoveRelation, RemoveAllowedType:
		return true
	default:
		return false
	}
}

func (m Mutation) String() string {
	switch m.Kind {
	case AddDefinition, RemoveDefinition:
		return m.Kind.String() + " " + m.Definition

	case AddAllowedType, RemoveAllowedType:
		if len(m.AllowedTypes) == 1 {
			return fmt.Sprintf("%s %s#%s: %s", m.Kind, m.Definition, m.Name, m.AllowedTypes[0])
		}
	}

	return fmt.Sprintf("%s %s#%s", m.Kind, m.Definition, m.Name)
}

// ApplyMutations returns a new schema consisting of the base schema with the mutations
// applied. The base schema is not modified. An error is returned if a mutation cannot be
// applied or if the resulting schema is not one that could be written.
func ApplyMutations(base *schema.Schema, mutations []Mutation) (*schema.Schema, error) {
	if err := checkSupported(base); err != nil {
		return nil, err
	}

	if err := checkOverlapping(mutations); err != nil {
		return nil, err
	}

	model := modelFromSchema(base)
	for _, mutation := range mutations {
		if err := model.apply(mutation); err != nil {
			return nil, fmt.Errorf("cannot apply %s: %w", mutation, err)
		}
	}

	built, err := model.build()
	if err != nil {
		return nil, err
	}

	if err := validateSchema(built); err != nil {
		return nil, fmt.Errorf("mutations produce an invalid schema: %w", err)
	}

	return built, nil
}

// checkOverlapping ensures that no two mutations target the same relation or permission,
// and that nothing else targets a definition that is being added or removed.
func checkOverlapping(mutations []Mutation) error {
	type target struct{ definition, name string }

	wholeDefinitions := map[string]Mutation{}
	for _, mutation := range mutations {
		if mutation.Kind != AddDefinition && mutation.Kind != RemoveDefinition {
			continue
		}
		if existing, ok := wholeDefinitions[mutation.Definition]; ok {
			return fmt.Errorf("overlapping mutations: %s and %s", existing, mutation)
		}
		wholeDefinitions[mutation.Definition] = mutation
	}

	targets := map[target]Mutation{}
	for _, mutation := range mutations {
		if mutation.Kind == AddDefinition || mutation.Kind == RemoveDefinition {
			continue
		}
		if existing, ok := wholeDefinitions[mutation.Definition]; ok {
			return fmt.Errorf("overlapping mutations: %s and %s", existing, mutation)
		}

		key := target{mutation.Definition, mutation.Name}
		if existing, ok := targets[key]; ok {
			return fmt.Errorf("overlapping mutations: %s and %s", existing, mutation)
		}
		targets[key] = mutation
	}

	return nil
}

// validateSchema ensures the schema is one that a schema write would accept.
func validateSchema(s *schema.Schema) error {
	if _, err := schema.ResolveSchema(s); err != nil {
		return err
	}

	compiled, err := s.AsCompiledSchema()
	if err != nil {
		return err
	}

	ts := schemapkg.NewTypeSystem(schemapkg.ResolverForSchema(compiled))
	for _, definitionName := range sortedKeys(s.Definitions()) {
		if _, err := ts.GetValidatedDefinition(context.Background(), definitionName); err != nil {
			return err
		}
	}

	return nil
}

// definitionModel is a plain, editable view of a definition.
type definitionModel struct {
	relations   map[string][]AllowedType
	permissions map[string]schema.Operation
}

// schemaModel is a plain, editable view of a schema, keyed by definition name. Operations
// found in a model are never modified, and are copied when a schema is built from it.
type schemaModel map[string]*definitionModel

func modelFromSchema(s *schema.Schema) schemaModel {
	model := make(schemaModel, len(s.Definitions()))
	for definitionName, definition := range s.Definitions() {
		definitionModel := &definitionModel{
			relations:   make(map[string][]AllowedType, len(definition.Relations())),
			permissions: make(map[string]schema.Operation, len(definition.Permissions())),
		}

		for relationName, relation := range definition.Relations() {
			allowedTypes := make([]AllowedType, 0, len(relation.BaseRelations()))
			for _, baseRelation := range relation.BaseRelations() {
				allowedTypes = append(allowedTypes, AllowedType{Type: baseRelation.Type(), Subrelation: baseRelation.Subrelation()})
			}
			definitionModel.relations[relationName] = allowedTypes
		}

		for permissionName, permission := range definition.Permissions() {
			definitionModel.permissions[permissionName] = permission.Operation()
		}

		model[definitionName] = definitionModel
	}
	return model
}

func (sm schemaModel) clone() schemaModel {
	cloned := make(schemaModel, len(sm))
	for definitionName, definition := range sm {
		clonedDefinition := &definitionModel{
			relations:   make(map[string][]AllowedType, len(definition.relations)),
			permissions: make(map[string]schema.Operation, len(definition.permissions)),
		}
		for relationName, allowedTypes := range definition.relations {
			clonedDefinition.relations[relationName] = slices.Clone(allowedTypes)
		}
		for permissionName, operation := range definition.permissions {
			clonedDefinition.permissions[permissionName] = operation
		}
		cloned[definitionName] = clonedDefinition
	}
	return cloned
}

// hasName returns whether the definition has a relation or permission with the name.
func (dm *definitionModel) hasName(name string) bool {
	_, isRelation := dm.relations[name]
	_, isPermission := dm.permissions[name]
	return isRelation || isPermission
}

// allows returns whether the relation exists and allows the subject type.
func (sm schemaModel) allows(definitionName, relationName string, allowedType AllowedType) bool {
	definition, ok := sm[definitionName]
	if !ok {
		return false
	}
	return slices.Contains(definition.relations[relationName], allowedType)
}

func (sm schemaModel) apply(mutation Mutation) error {
	if mutation.Kind == AddDefinition {
		if _, ok := sm[mutation.Definition]; ok {
			return fmt.Errorf("definition `%s` already exists", mutation.Definition)
		}

		sm[mutation.Definition] = &definitionModel{
			relations:   map[string][]AllowedType{},
			permissions: map[string]schema.Operation{},
		}

		for _, member := range mutation.Members {
			if member.Definition != mutation.Definition || (member.Kind != AddRelation && member.Kind != AddPermission) {
				return fmt.Errorf("invalid member %s", member)
			}
			if err := sm.apply(member); err != nil {
				return err
			}
		}
		return nil
	}

	definition, ok := sm[mutation.Definition]
	if !ok {
		return fmt.Errorf("definition `%s` not found", mutation.Definition)
	}

	switch mutation.Kind {
	case RemoveDefinition:
		delete(sm, mutation.Definition)

	case AddRelation:
		if definition.hasName(mutation.Name) {
			return fmt.Errorf("`%s` already exists", mutation.Name)
		}
		if len(mutation.AllowedTypes) == 0 {
			return errors.New("a relation requires at least one allowed type")
		}
		definition.relations[mutation.Name] = slices.Clone(mutation.AllowedTypes)

	case RemoveRelation:
		if _, ok := definition.relations[mutation.Name]; !ok {
			return fmt.Errorf("relation `%s` not found", mutation.Name)
		}
		delete(definition.relations, mutation.Name)

	case AddAllowedType, RemoveAllowedType:
		allowedTypes, ok := definition.relations[mutation.Name]
		if !ok {
			return fmt.Errorf("relation `%s` not found", mutation.Name)
		}
		if len(mutation.AllowedTypes) != 1 {
			return errors.New("exactly one allowed type is required")
		}

		allowedType := mutation.AllowedTypes[0]
		index := slices.Index(allowedTypes, allowedType)
		if mutation.Kind == AddAllowedType {
			if index >= 0 {
				return fmt.Errorf("allowed type `%s` already exists", allowedType)
			}
			definition.relations[mutation.Name] = append(slices.Clone(allowedTypes), allowedType)
			return nil
		}

		if index < 0 {
			return fmt.Errorf("allowed type `%s` not found", allowedType)
		}
		if len(allowedTypes) == 1 {
			return fmt.Errorf("cannot remove the last allowed type `%s`", allowedType)
		}
		definition.relations[mutation.Name] = slices.Delete(slices.Clone(allowedTypes), index, index+1)

	case AddPermission:
		if definition.hasName(mutation.Name) {
			return fmt.Errorf("`%s` already exists", mutation.Name)
		}
		if mutation.Operation == nil {
			return errors.New("a permission requires an operation")
		}
		definition.permissions[mutation.Name] = mutation.Operation

	case RemovePermission:
		if _, ok := definition.permissions[mutation.Name]; !ok {
			return fmt.Errorf("permission `%s` not found", mutation.Name)
		}
		delete(definition.permissions, mutation.Name)

	case ChangePermission:
		if _, ok := definition.permissions[mutation.Name]; !ok {
			return fmt.Errorf("permission `%s` not found", mutation.Name)
		}
		if mutation.Operation == nil {
			return errors.New("a permission requires an operation")
		}
		definition.permissions[mutation.Name] = mutation.Operation

	case AddUnionChild, RemoveUnionChild:
		existing, ok := definition.permissions[mutation.Name]
		if !ok {
			return fmt.Errorf("permission `%s` not found", mutation.Name)
		}
		if mutation.Operation == nil {
			return errors.New("a union child requires an operation")
		}

		updated, err := addUnionChild(existing, mutation.Operation)
		if mutation.Kind == RemoveUnionChild {
			updated, err = removeUnionChild(existing, mutation.Operation)
		}
		if err != nil {
			return fmt.Errorf("permission `%s`: %w", mutation.Name, err)
		}
		definition.permissions[mutation.Name] = updated

	default:
		return fmt.Errorf("unknown mutation kind %d", int(mutation.Kind))
	}

	return nil
}

// build constructs a new schema from the model.
func (sm schemaModel) build() (*schema.Schema, error) {
	builder := schema.NewSchemaBuilder()
	for _, definitionName := range sortedKeys(sm) {
		definition := sm[definitionName]
		definitionBuilder := builder.AddDefinition(definitionName)

		for _, relationName := range sortedKeys(definition.relations) {
			relationBuilder := definitionBuilder.AddRelation(relationName)
			// NOTE: the allowed types are sorted so that the schema built does not depend
			// upon the order in which they were added.
			allowedTypes := slices.Clone(definition.relations[relationName])
			slices.SortFunc(allowedTypes, func(a, b AllowedType) int {
				return cmp.Or(cmp.Compare(a.Type, b.Type), cmp.Compare(a.Subrelation, b.Subrelation))
			})

			for _, allowedType := range allowedTypes {
				if allowedType.Subrelation == tuple.Ellipsis {
					relationBuilder = relationBuilder.AllowedDirectRelation(allowedType.Type)
				} else {
					relationBuilder = relationBuilder.AllowedRelation(allowedType.Type, allowedType.Subrelation)
				}
			}
			definitionBuilder = relationBuilder.Done()
		}

		for _, permissionName := range sortedKeys(definition.permissions) {
			operation, err := copyOperation(definition.permissions[permissionName])
			if err != nil {
				return nil, fmt.Errorf("%s#%s: %w", definitionName, permissionName, err)
			}
			definitionBuilder = definitionBuilder.AddPermission(permissionName).Operation(operation).Done()
		}

		builder = definitionBuilder.Done()
	}
	return builder.Build(), nil
}

// unionChildren returns the children of the operation if it is a union, or nil otherwise.
func unionChildren(op schema.Operation) []schema.Operation {
	if union, ok := op.(*schema.UnionOperation); ok {
		return union.Children()
	}
	return nil
}

// addUnionChild returns the operation with the child added to the union at its root. An
// operation that is not a union becomes a union of the operation and the child.
func addUnionChild(existing, child schema.Operation) (schema.Operation, error) {
	children := unionChildren(existing)
	if children == nil {
		children = []schema.Operation{existing}
	}
	return unionOf(append(slices.Clone(children), child))
}

// removeUnionChild returns the operation with the last child identical to the one given
// removed from the union at its root. A sole remaining child takes the place of the union.
func removeUnionChild(existing, child schema.Operation) (schema.Operation, error) {
	children := unionChildren(existing)
	if len(children) < 2 {
		return nil, errors.New("not a union with multiple children")
	}

	index := lastIdenticalIndex(children, child)
	if index < 0 {
		return nil, errors.New("union child not found")
	}

	remaining := slices.Delete(slices.Clone(children), index, index+1)
	if len(remaining) == 1 {
		return remaining[0], nil
	}
	return unionOf(remaining)
}

// unionOf returns a union of copies of the operations.
func unionOf(children []schema.Operation) (schema.Operation, error) {
	copied := make([]schema.Operation, 0, len(children))
	for _, child := range children {
		copiedChild, err := copyOperation(child)
		if err != nil {
			return nil, err
		}
		copied = append(copied, copiedChild)
	}
	return schema.Union(copied...), nil
}

// lastIdenticalIndex returns the index of the last operation identical to the one given,
// or -1 if there is none.
func lastIdenticalIndex(ops []schema.Operation, op schema.Operation) int {
	for index := len(ops) - 1; index >= 0; index-- {
		if operationsIdentical(ops[index], op) {
			return index
		}
	}
	return -1
}

// copyOperation returns an unresolved copy of the operation, sharing nothing with it.
func copyOperation(op schema.Operation) (schema.Operation, error) {
	return mapOperation(op, func(leaf schema.Operation) (schema.Operation, error) {
		return nil, nil
	})
}

// mapOperation returns an unresolved copy of the operation. The replacer is invoked for
// each relation reference and arrow, and can return a non-nil operation to use in place
// of a copy of that leaf.
func mapOperation(op schema.Operation, replacer func(leaf schema.Operation) (schema.Operation, error)) (schema.Operation, error) {
	mapChildren := func(children []schema.Operation) ([]schema.Operation, error) {
		mapped := make([]schema.Operation, 0, len(children))
		for _, child := range children {
			mappedChild, err := mapOperation(child, replacer)
			if err != nil {
				return nil, err
			}
			mapped = append(mapped, mappedChild)
		}
		return mapped, nil
	}

	switch typed := op.(type) {
	case *schema.RelationReference, *schema.ResolvedRelationReference, *schema.ArrowReference, *schema.ResolvedArrowReference:
		replacement, err := replacer(op)
		if err != nil || replacement != nil {
			return replacement, err
		}

		if relationName, ok := relationReferenceName(op); ok {
			return schema.NewRelationRef(relationName), nil
		}

		left, right, _ := arrowNames(op)
		return schema.NewArrow(left, right), nil

	case *schema.UnionOperation:
		children, err := mapChildren(typed.Children())
		if err != nil {
			return nil, err
		}
		return schema.Union(children...), nil

	case *schema.IntersectionOperation:
		children, err := mapChildren(typed.Children())
		if err != nil {
			return nil, err
		}
		return schema.Intersection(children...), nil

	case *schema.ExclusionOperation:
		children, err := mapChildren([]schema.Operation{typed.Left(), typed.Right()})
		if err != nil {
			return nil, err
		}
		return schema.Exclusion(children[0], children[1]), nil

	default:
		return nil, fmt.Errorf("unsupported operation %T", op)
	}
}

// relationReferenceName returns the name of the relation or permission referenced by the
// operation, if it is a relation reference.
func relationReferenceName(op schema.Operation) (string, bool) {
	switch typed := op.(type) {
	case *schema.RelationReference:
		return typed.RelationName(), true
	case *schema.ResolvedRelationReference:
		return typed.RelationName(), true
	default:
		return "", false
	}
}

// arrowNames returns the left and right sides of the operation, if it is an arrow.
func arrowNames(op schema.Operation) (string, string, bool) {
	switch typed := op.(type) {
	case *schema.ArrowReference:
		return typed.Left(), typed.Right(), true
	case *schema.ResolvedArrowReference:
		return typed.Left(), typed.Right(), true
	default:
		return "", "", false
	}
}
