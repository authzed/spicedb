package tuple

import (
	"time"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// ONRStringToCore creates an ONR from string pieces.
func ONRStringToCore(ns, oid, rel string) *core.ObjectAndRelation {
	spiceerrors.DebugAssertf(func() bool {
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
	if rel.GetSubject().GetRelation() == Ellipsis {
		return rel.GetResourceAndRelation().GetNamespace() + ":" + rel.GetResourceAndRelation().GetObjectId() + "@" + rel.GetSubject().GetNamespace() + ":" + rel.GetSubject().GetObjectId()
	}

	return rel.GetResourceAndRelation().GetNamespace() + ":" + rel.GetResourceAndRelation().GetObjectId() + "@" + rel.GetSubject().GetNamespace() + ":" + rel.GetSubject().GetObjectId() + "#" + rel.GetResourceAndRelation().GetRelation()
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
	spiceerrors.DebugAssertf(func() bool {
		return namespaceName != "" && relationName != ""
	}, "namespace and relation must not be empty")

	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}

// FromCoreRelationTuple creates a Relationship from a core.RelationTuple.
func FromCoreRelationTuple(rt *core.RelationTuple) Relationship {
	spiceerrors.DebugAssertf(func() bool {
		return rt.Validate() == nil
	}, "relation tuple must be valid")

	var expiration *time.Time
	if rt.GetOptionalExpirationTime() != nil {
		t := rt.GetOptionalExpirationTime().AsTime()
		expiration = &t
	}

	return Relationship{
		RelationshipReference: RelationshipReference{
			Resource: ObjectAndRelation{
				ObjectType: rt.GetResourceAndRelation().GetNamespace(),
				ObjectID:   rt.GetResourceAndRelation().GetObjectId(),
				Relation:   rt.GetResourceAndRelation().GetRelation(),
			},
			Subject: ObjectAndRelation{
				ObjectType: rt.GetSubject().GetNamespace(),
				ObjectID:   rt.GetSubject().GetObjectId(),
				Relation:   rt.GetSubject().GetRelation(),
			},
		},
		OptionalCaveat:     rt.GetCaveat(),
		OptionalExpiration: expiration,
	}
}

// FromCoreObjectAndRelation creates an ObjectAndRelation from a core.ObjectAndRelation.
func FromCoreObjectAndRelation(oar *core.ObjectAndRelation) ObjectAndRelation {
	spiceerrors.DebugAssertf(func() bool {
		return oar.Validate() == nil
	}, "object and relation must be valid")

	return ObjectAndRelation{
		ObjectType: oar.GetNamespace(),
		ObjectID:   oar.GetObjectId(),
		Relation:   oar.GetRelation(),
	}
}

// CoreONR creates a core ObjectAndRelation from the string pieces.
func CoreONR(namespace, objectID, relation string) *core.ObjectAndRelation {
	spiceerrors.DebugAssertf(func() bool {
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
	spiceerrors.DebugAssertf(func() bool {
		return namespace != "" && relation != ""
	}, "namespace and relation must not be empty")

	return &core.RelationReference{
		Namespace: namespace,
		Relation:  relation,
	}
}

// FromCoreRelationReference creates a RelationReference from a core.RelationReference.
func FromCoreRelationReference(rr *core.RelationReference) RelationReference {
	spiceerrors.DebugAssertf(func() bool {
		return rr.Validate() == nil
	}, "relation reference must be valid")

	return RelationReference{
		ObjectType: rr.GetNamespace(),
		Relation:   rr.GetRelation(),
	}
}
