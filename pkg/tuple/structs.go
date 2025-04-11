package tuple

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
	ObjectData *structpb.Struct
	Relation   string
}

const onrStructSize = 56 /* size of the struct itself */

func (onr ObjectAndRelation) SizeVT() int {
	size := len(onr.ObjectID) + len(onr.ObjectType) + len(onr.Relation) + onrStructSize
	if onr.ObjectData != nil {
		props, _ := onr.ObjectData.MarshalJSON()
		size += len(props)
	}
	return size
}

// WithRelation returns a copy of the object and relation with the given relation.
func (onr ObjectAndRelation) WithRelation(relation string) ObjectAndRelation {
	onr.Relation = relation
	return onr
}

// RelationReference returns a RelationReference for the object and relation.
func (onr ObjectAndRelation) RelationReference() RelationReference {
	return RelationReference{
		ObjectType: onr.ObjectType,
		Relation:   onr.Relation,
	}
}

// ToCoreONR converts the ObjectAndRelation to a core.ObjectAndRelation.
func (onr ObjectAndRelation) ToCoreONR() *core.ObjectAndRelation {
	return &core.ObjectAndRelation{
		Namespace:  onr.ObjectType,
		ObjectId:   onr.ObjectID,
		ObjectData: onr.ObjectData,
		Relation:   onr.Relation,
	}
}

func (onr ObjectAndRelation) String() string {
	return fmt.Sprintf("%s:%s#%s", onr.ObjectType, onr.ObjectID, onr.Relation)
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
	OptionalCaveat     *core.ContextualizedCaveat
	OptionalExpiration *time.Time
	OptionalIntegrity  *core.RelationshipIntegrity

	RelationshipReference
}

// ToCoreTuple converts the Relationship to a core.RelationTuple.
func (r Relationship) ToCoreTuple() *core.RelationTuple {
	var expirationTime *timestamppb.Timestamp
	if r.OptionalExpiration != nil {
		expirationTime = timestamppb.New(*r.OptionalExpiration)
	}

	return &core.RelationTuple{
		ResourceAndRelation:    r.Resource.ToCoreONR(),
		Subject:                r.Subject.ToCoreONR(),
		Caveat:                 r.OptionalCaveat,
		Integrity:              r.OptionalIntegrity,
		OptionalExpirationTime: expirationTime,
	}
}

const relStructSize = 136 /* size of the struct itself */

func (r Relationship) SizeVT() int {
	size := r.Resource.SizeVT() + r.Subject.SizeVT() + relStructSize
	if r.OptionalCaveat != nil {
		size += r.OptionalCaveat.SizeVT()
	}
	return size
}

// ValidateNotEmpty returns true if the relationship is not empty.
func (r Relationship) ValidateNotEmpty() bool {
	return r.Resource.ObjectType != "" && r.Resource.ObjectID != "" && r.Subject.ObjectType != "" && r.Subject.ObjectID != "" && r.Resource.Relation != "" && r.Subject.Relation != ""
}

// Validate returns an error if the relationship is invalid.
func (r Relationship) Validate() error {
	if !r.ValidateNotEmpty() {
		return errors.New("object and relation must not be empty")
	}

	if r.RelationshipReference.Resource.ObjectID == PublicWildcard {
		return errors.New("invalid resource id")
	}

	return nil
}

// WithoutIntegrity returns a copy of the relationship without its integrity.
func (r Relationship) WithoutIntegrity() Relationship {
	r.OptionalIntegrity = nil
	return r
}

// WithCaveat returns a copy of the relationship with the given caveat.
func (r Relationship) WithCaveat(caveat *core.ContextualizedCaveat) Relationship {
	r.OptionalCaveat = caveat
	return r
}

// UpdateOperation represents the type of update to a relationship.
type UpdateOperation int

const (
	UpdateOperationTouch UpdateOperation = iota
	UpdateOperationCreate
	UpdateOperationDelete
)

// RelationshipUpdate represents an update to a relationship.
type RelationshipUpdate struct {
	Relationship Relationship
	Operation    UpdateOperation
}

func (ru RelationshipUpdate) OperationString() string {
	switch ru.Operation {
	case UpdateOperationTouch:
		return "TOUCH"
	case UpdateOperationCreate:
		return "CREATE"
	case UpdateOperationDelete:
		return "DELETE"
	default:
		return "unknown"
	}
}

func (ru RelationshipUpdate) DebugString() string {
	return fmt.Sprintf("%s(%s)", ru.OperationString(), StringWithoutCaveatOrExpiration(ru.Relationship))
}

// RelationReference represents a reference to a relation.
type RelationReference struct {
	ObjectType string
	Relation   string
}

// ToCoreRR converts the RelationReference to a core.RelationReference.
func (rr RelationReference) ToCoreRR() *core.RelationReference {
	return &core.RelationReference{
		Namespace: rr.ObjectType,
		Relation:  rr.Relation,
	}
}

func (rr RelationReference) RefString() string {
	return JoinRelRef(rr.ObjectType, rr.Relation)
}

func (rr RelationReference) String() string {
	return rr.RefString()
}

// ONR creates an ObjectAndRelation.
func ONR(namespace, objectID, relation string, objectData ...*structpb.Struct) ObjectAndRelation {
	spiceerrors.DebugAssert(func() bool {
		return namespace != "" && objectID != "" && relation != ""
	}, "invalid ONR: %s %s %s", namespace, objectID, relation)
	onr := ObjectAndRelation{
		ObjectType: namespace,
		ObjectID:   objectID,
		Relation:   relation,
	}
	if len(objectData) > 0 {
		onr.ObjectData = objectData[0]
	}
	return onr
}

// ONRRef creates an ObjectAndRelation reference.
func ONRRef(namespace, objectID, relation string, objectData ...*structpb.Struct) *ObjectAndRelation {
	onr := ONR(namespace, objectID, relation, objectData...)
	return &onr
}

// RR creates a RelationReference.
func RR(namespace, relation string) RelationReference {
	spiceerrors.DebugAssert(func() bool {
		return namespace != "" && relation != ""
	}, "invalid RR: %s %s", namespace, relation)

	return RelationReference{
		ObjectType: namespace,
		Relation:   relation,
	}
}
