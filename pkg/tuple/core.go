package tuple

import (
	"time"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ONRStringToCore creates an ONR from string pieces.
func ONRStringToCore(ns, oid, rel string) *core.ObjectAndRelation {
	spiceerrors.DebugAssert(func() bool {
		return ns != "" && oid != "" && rel != ""
	}, "namespace, object ID, and relation must not be empty")

	return &core.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
}

// CoreRelationToStringWithoutCaveatOrExpiration creates a string from a core.RelationTuple without stringifying the caveat.
func CoreRelationToStringWithoutCaveatOrExpiration(rel *core.RelationTuple) string {
	if rel.Subject.Relation == Ellipsis {
		return rel.ResourceAndRelation.Namespace + ":" + rel.ResourceAndRelation.ObjectId + "@" + rel.Subject.Namespace + ":" + rel.Subject.ObjectId
	}

	return rel.ResourceAndRelation.Namespace + ":" + rel.ResourceAndRelation.ObjectId + "@" + rel.Subject.Namespace + ":" + rel.Subject.ObjectId + "#" + rel.ResourceAndRelation.Relation
}

// CoreRelationToString creates a string from a core.RelationTuple.
func CoreRelationToString(rel *core.RelationTuple) (string, error) {
	return String(FromCoreRelationTuple(rel))
}

// MustCoreRelationToString creates a string from a core.RelationTuple and panics if it can't.
func MustCoreRelationToString(rel *core.RelationTuple) string {
	return MustString(FromCoreRelationTuple(rel))
}

// RRStringToCore creates a RelationReference from the string pieces.
func RRStringToCore(namespaceName string, relationName string) *core.RelationReference {
	spiceerrors.DebugAssert(func() bool {
		return namespaceName != "" && relationName != ""
	}, "namespace and relation must not be empty")

	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

// FromCoreRelationTuple creates a Relationship from a core.RelationTuple.
func FromCoreRelationTuple(rt *core.RelationTuple) Relationship {
	spiceerrors.DebugAssert(func() bool {
		return rt.Validate() == nil
	}, "relation tuple must be valid")

	var expiration *time.Time
	if rt.OptionalExpirationTime != nil {
		t := rt.OptionalExpirationTime.AsTime()
		expiration = &t
	}

	return Relationship{
		RelationshipReference: RelationshipReference{
			Resource: ObjectAndRelation{
				ObjectType: rt.ResourceAndRelation.Namespace,
				ObjectID:   rt.ResourceAndRelation.ObjectId,
				Relation:   rt.ResourceAndRelation.Relation,
			},
			Subject: ObjectAndRelation{
				ObjectType: rt.Subject.Namespace,
				ObjectID:   rt.Subject.ObjectId,
				Relation:   rt.Subject.Relation,
			},
		},
		OptionalCaveat:     rt.Caveat,
		OptionalExpiration: expiration,
	}
}

// FromCoreObjectAndRelation creates an ObjectAndRelation from a core.ObjectAndRelation.
func FromCoreObjectAndRelation(oar *core.ObjectAndRelation) ObjectAndRelation {
	spiceerrors.DebugAssert(func() bool {
		return oar.Validate() == nil
	}, "object and relation must be valid")

	return ObjectAndRelation{
		ObjectType: oar.Namespace,
		ObjectID:   oar.ObjectId,
		Relation:   oar.Relation,
	}
}

// CoreONR creates a core ObjectAndRelation from the string pieces.
func CoreONR(namespace, objectID, relation string) *core.ObjectAndRelation {
	spiceerrors.DebugAssert(func() bool {
		return namespace != "" && objectID != "" && relation != ""
	}, "namespace, object ID, and relation must not be empty")

	return &core.ObjectAndRelation{
		Namespace: namespace,
		ObjectId:  objectID,
		Relation:  relation,
	}
}

// CoreRR creates a core RelationReference from the string pieces.
func CoreRR(namespace, relation string) *core.RelationReference {
	spiceerrors.DebugAssert(func() bool {
		return namespace != "" && relation != ""
	}, "namespace and relation must not be empty")

	return &core.RelationReference{
		Namespace: namespace,
		Relation:  relation,
	}
}

// FromCoreRelationshipReference creates a RelationshipReference from a core.RelationshipReference.
func FromCoreRelationReference(rr *core.RelationReference) RelationReference {
	spiceerrors.DebugAssert(func() bool {
		return rr.Validate() == nil
	}, "relation reference must be valid")

	return RelationReference{
		ObjectType: rr.Namespace,
		Relation:   rr.Relation,
	}
}
