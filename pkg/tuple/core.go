package tuple

import (
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

// RelationReference creates a RelationReference from the string pieces.
func RelationReference(namespaceName string, relationName string) *core.RelationReference {
	spiceerrors.DebugAssert(func() bool {
		return namespaceName != "" && relationName != ""
	}, "namespace and relation must not be empty")

	return &core.RelationReference{
		Namespace: namespaceName,
		Relation:  relationName,
	}
}
