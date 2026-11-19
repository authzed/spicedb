package testing

import (
	"slices"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// Variant is a schema derived from a base schema by applying a set of mutations.
type Variant struct {
	// Base is the schema the variant was derived from. It is a normalized copy of the
	// schema given: comments and other metadata are not carried over, so that the only
	// differences between Base and Schema are those named in Mutations.
	Base *schema.Schema

	// Schema is Base with Mutations applied.
	Schema *schema.Schema

	// Mutations are all of the changes between Base and Schema.
	Mutations []Mutation

	// Relationships generates relationships for Schema. It generates no relationships if
	// Schema has no relations.
	Relationships RelationshipGenerator

	// baseRelationships generates relationships for Base.
	baseRelationships RelationshipGenerator

	baseModel schemaModel
	model     schemaModel

	// objectIDs are the object IDs to use in generated relationships, if any.
	objectIDs []string
}

// NewVariant returns the variant of the base schema produced by applying the mutations.
func NewVariant(base *schema.Schema, mutations []Mutation) (Variant, error) {
	normalizedBase, err := ApplyMutations(base, nil)
	if err != nil {
		return Variant{}, err
	}

	mutated, err := ApplyMutations(base, mutations)
	if err != nil {
		return Variant{}, err
	}

	v := Variant{
		Base:      normalizedBase,
		Schema:    mutated,
		Mutations: mutations,
		baseModel: modelFromSchema(normalizedBase),
		model:     modelFromSchema(mutated),
	}

	v.baseRelationships, err = relationshipGeneratorFor(normalizedBase, v.baseModel)
	if err != nil {
		return Variant{}, err
	}

	v.Relationships, err = relationshipGeneratorFor(mutated, v.model)
	if err != nil {
		return Variant{}, err
	}

	return v, nil
}

// relationshipGeneratorFor returns the generator of relationships for the schema, which is
// the zero value if the schema has no relations.
func relationshipGeneratorFor(s *schema.Schema, model schemaModel) (RelationshipGenerator, error) {
	for _, definition := range model {
		if len(definition.relations) > 0 {
			return NewRelationshipGenerator(s)
		}
	}
	return RelationshipGenerator{}, nil
}

// WithObjectIDs returns a copy of the variant whose generated relationships draw the IDs of
// resources and subjects from the given object IDs, rather than randomly generating them.
// It applies to Relationships, GenerateRemovedRelationships and GenerateAddedRelationships.
func (v Variant) WithObjectIDs(objectIDs ...string) Variant {
	v.objectIDs = slices.Clone(objectIDs)
	v.Relationships = v.Relationships.WithObjectIDs(objectIDs...)
	v.baseRelationships = v.baseRelationships.WithObjectIDs(objectIDs...)
	return v
}

// Inverse returns the variant that undoes this one: its Base is this variant's Schema, its
// Schema is this variant's Base, and its Mutations are those that change the one into the
// other. It can be used to model a change to a schema being rolled back.
//
// Adding a child to a union is undone by removing it, and the reverse, only where doing so
// produces exactly the expression found in Base, such as when the child removed was the
// last of the union. Otherwise, it is undone by a ChangePermission.
func (v Variant) Inverse() Variant {
	mutations := make([]Mutation, 0, len(v.Mutations))
	for _, mutation := range slices.Backward(v.Mutations) {
		mutations = append(mutations, v.inverseOf(mutation))
	}

	return Variant{
		Base:              v.Schema,
		Schema:            v.Base,
		Mutations:         mutations,
		Relationships:     v.baseRelationships,
		baseRelationships: v.Relationships,
		baseModel:         v.model,
		model:             v.baseModel,
		objectIDs:         v.objectIDs,
	}
}

// inverseOf returns the mutation that undoes the given mutation of the variant.
func (v Variant) inverseOf(mutation Mutation) Mutation {
	inverse := Mutation{Definition: mutation.Definition, Name: mutation.Name}
	baseDefinition := v.baseModel[mutation.Definition]

	switch mutation.Kind {
	case AddDefinition:
		inverse.Kind = RemoveDefinition

	case RemoveDefinition:
		inverse.Kind = AddDefinition
		for _, relationName := range sortedKeys(baseDefinition.relations) {
			inverse.Members = append(inverse.Members, Mutation{
				Kind:         AddRelation,
				Definition:   mutation.Definition,
				Name:         relationName,
				AllowedTypes: slices.Clone(baseDefinition.relations[relationName]),
			})
		}
		for _, permissionName := range sortedKeys(baseDefinition.permissions) {
			inverse.Members = append(inverse.Members, Mutation{
				Kind:       AddPermission,
				Definition: mutation.Definition,
				Name:       permissionName,
				Operation:  baseDefinition.permissions[permissionName],
			})
		}

	case AddRelation:
		inverse.Kind = RemoveRelation

	case RemoveRelation:
		inverse.Kind = AddRelation
		inverse.AllowedTypes = slices.Clone(baseDefinition.relations[mutation.Name])

	case AddAllowedType:
		inverse.Kind = RemoveAllowedType
		inverse.AllowedTypes = slices.Clone(mutation.AllowedTypes)

	case RemoveAllowedType:
		inverse.Kind = AddAllowedType
		inverse.AllowedTypes = slices.Clone(mutation.AllowedTypes)

	case AddPermission:
		inverse.Kind = RemovePermission

	case RemovePermission:
		inverse.Kind = AddPermission
		inverse.Operation = baseDefinition.permissions[mutation.Name]

	case ChangePermission:
		inverse.Kind = ChangePermission
		inverse.Operation = baseDefinition.permissions[mutation.Name]

	case AddUnionChild, RemoveUnionChild:
		// NOTE: adding a child places it last, and removing one can replace the union with
		// its sole remaining child, so the opposite mutation is only used if it produces
		// exactly the expression found in the base.
		existing := baseDefinition.permissions[mutation.Name]
		updated := v.model[mutation.Definition].permissions[mutation.Name]

		inverse.Kind = RemoveUnionChild
		undone, err := removeUnionChild(updated, mutation.Operation)
		if mutation.Kind == RemoveUnionChild {
			inverse.Kind = AddUnionChild
			undone, err = addUnionChild(updated, mutation.Operation)
		}

		inverse.Operation = mutation.Operation
		if err != nil || !operationsIdentical(existing, undone) {
			inverse.Kind = ChangePermission
			inverse.Operation = existing
		}
	}

	return inverse
}

// Invalidated returns the mutations that cause a relationship that is valid under Base to
// no longer be valid under Schema. It is empty if the relationship is still valid. More
// than one mutation can be responsible for the same relationship.
func (v Variant) Invalidated(rel tuple.Relationship) []Mutation {
	var responsible []Mutation
	for _, mutation := range v.Mutations {
		if mutationInvalidates(mutation, rel) {
			responsible = append(responsible, mutation)
		}
	}
	return responsible
}

func mutationInvalidates(mutation Mutation, rel tuple.Relationship) bool {
	switch mutation.Kind {
	case RemoveDefinition:
		return rel.Resource.ObjectType == mutation.Definition || rel.Subject.ObjectType == mutation.Definition

	case RemoveRelation:
		if rel.Resource.ObjectType == mutation.Definition && rel.Resource.Relation == mutation.Name {
			return true
		}
		return rel.Subject.ObjectType == mutation.Definition && rel.Subject.Relation == mutation.Name

	case RemoveAllowedType:
		if len(mutation.AllowedTypes) != 1 {
			return false
		}
		return rel.Resource.ObjectType == mutation.Definition &&
			rel.Resource.Relation == mutation.Name &&
			rel.Subject.ObjectType == mutation.AllowedTypes[0].Type &&
			rel.Subject.Relation == mutation.AllowedTypes[0].Subrelation

	default:
		return false
	}
}

// Partition splits relationships that are valid under Base into those that are still valid
// under Schema and those that must be removed before Schema can be written.
func (v Variant) Partition(rels []tuple.Relationship) (kept, removed []tuple.Relationship) {
	for _, rel := range rels {
		if len(v.Invalidated(rel)) > 0 {
			removed = append(removed, rel)
		} else {
			kept = append(kept, rel)
		}
	}
	return kept, removed
}
