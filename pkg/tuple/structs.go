package tuple

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	// Ellipsis is the Ellipsis relation in v0 style subjects.
	Ellipsis = "..."

	// PublicWildcard is the wildcard value for subject object IDs that indicates public access
	// for the subject type.
	PublicWildcard = "*"
)

// ObjectAndRelation represents an object and its relation.
type ObjectAndRelation struct {
	ObjectID   string
	ObjectType string
	Relation   string
}

const onrStructSize = 48 /* size of the struct itself */

func (onr ObjectAndRelation) SizeVT() int {
	return len(onr.ObjectID) + len(onr.ObjectType) + len(onr.Relation) + onrStructSize
}

// RelationshipReference represents a reference to a relationship, i.e. those portions
// of a relationship that are not the integrity or caveat and thus form the unique
// identifier of the relationship.
type RelationshipReference struct {
	Resource ObjectAndRelation
	Subject  ObjectAndRelation
}

// Relationship represents a relationship between two objects.
type Relationship struct {
	RelationshipReference
	OptionalCaveat    *core.ContextualizedCaveat
	OptionalIntegrity *core.RelationshipIntegrity
}

const relStructSize = 112 /* size of the struct itself */

func (r Relationship) SizeVT() int {
	size := r.Resource.SizeVT() + r.Subject.SizeVT() + relStructSize
	if r.OptionalCaveat != nil {
		size += r.OptionalCaveat.SizeVT()
	}
	return size
}

func (r Relationship) ValidateNotEmpty() bool {
	return r.Resource.ObjectType != "" && r.Resource.ObjectID != "" && r.Subject.ObjectType != "" && r.Subject.ObjectID != "" && r.Resource.Relation != "" && r.Subject.Relation != ""
}

// WithoutIntegrity returns a copy of the relationship without its integrity.
func (r Relationship) WithoutIntegrity() Relationship {
	r.OptionalIntegrity = nil
	return r
}

type UpdateOperation int

const (
	UpdateOperationTouch UpdateOperation = iota
	UpdateOperationCreate
	UpdateOperationDelete
)

// RelationshipUpdate represents an update to a relationship.
type RelationshipUpdate struct {
	Operation    UpdateOperation
	Relationship Relationship
}
