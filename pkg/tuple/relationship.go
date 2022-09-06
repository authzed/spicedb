package tuple

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// StringObjectRef marshals a *v1.ObjectReference into a string.
func StringObjectRef(ref *v1.ObjectReference) string {
	return ref.ObjectType + ":" + ref.ObjectId
}

// StringSubjectRef marshals a *v1.SubjectReference into a string.
func StringSubjectRef(ref *v1.SubjectReference) string {
	if ref.OptionalRelation != "" {
		return ref.Object.ObjectType + ":" + ref.Object.ObjectId + "#" + ref.OptionalRelation
	}
	return ref.Object.ObjectType + ":" + ref.Object.ObjectId
}

// StringRelationship converts a v1.Relationship to a string.
func StringRelationship(rel *v1.Relationship) string {
	if rel == nil || rel.Resource == nil || rel.Subject == nil {
		return ""
	}
	return StringObjectRef(rel.Resource) + "#" + rel.Relation + "@" + StringSubjectRef(rel.Subject)
}
