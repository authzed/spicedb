package query

import "github.com/authzed/spicedb/pkg/tuple"

type ObjectAndRelation = tuple.ObjectAndRelation

// Object represents a single object, without specifying the relation.
type Object struct {
	ObjectID   string
	ObjectType string
}

// WithRelation builds a full ObjectAndRelation out of the given Object.
func (o Object) WithRelation(relation string) ObjectAndRelation {
	return ObjectAndRelation{
		ObjectID:   o.ObjectID,
		ObjectType: o.ObjectType,
		Relation:   relation,
	}
}

// WithEllipses builds an ObjectAndRelation from an object with the default ellipses relation.
func (o Object) WithEllipses() ObjectAndRelation {
	return ObjectAndRelation{
		ObjectID:   o.ObjectID,
		ObjectType: o.ObjectType,
		Relation:   tuple.Ellipsis,
	}
}

func (o Object) Equals(other Object) bool {
	return o.ObjectID == other.ObjectID && o.ObjectType == other.ObjectType
}

// Key returns a unique string key for this Object
func (o Object) Key() string {
	return o.ObjectType + ":" + o.ObjectID
}

// NewObject creates a new Object with the given object type and ID.
func NewObject(objectType, objectID string) Object {
	return Object{
		ObjectID:   objectID,
		ObjectType: objectType,
	}
}

// NewObjects creates a slice of Objects of the same type with the given object IDs.
func NewObjects(objectType string, objectIDs ...string) []Object {
	objects := make([]Object, len(objectIDs))
	for i, objectID := range objectIDs {
		objects[i] = Object{
			ObjectID:   objectID,
			ObjectType: objectType,
		}
	}
	return objects
}

// NewObjectAndRelation creates a new ObjectAndRelation with the given object ID, type, and relation.
func NewObjectAndRelation(objectID, objectType, relation string) ObjectAndRelation {
	return ObjectAndRelation{
		ObjectID:   objectID,
		ObjectType: objectType,
		Relation:   relation,
	}
}

// GetObject extracts the Object part from an ObjectAndRelation.
func GetObject(oar ObjectAndRelation) Object {
	return Object{
		ObjectID:   oar.ObjectID,
		ObjectType: oar.ObjectType,
	}
}

// ObjectAndRelationKey returns a unique string key for an ObjectAndRelation
func ObjectAndRelationKey(oar ObjectAndRelation) string {
	return oar.ObjectType + ":" + oar.ObjectID + "#" + oar.Relation
}
