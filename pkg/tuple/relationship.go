package tuple

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// JoinObject joins the namespace and the objectId together into the standard
// format.
//
// This function assumes that the provided values have already been validated.
func JoinObjectRef(namespace, objectID string) string { return namespace + ":" + objectID }

// StringObjectRef marshals a *v1.ObjectReference into a string.
//
// This function assumes that the provided values have already been validated.
func StringObjectRef(ref *v1.ObjectReference) string {
	return JoinObjectRef(ref.ObjectType, ref.ObjectId)
}

// StringSubjectRef marshals a *v1.SubjectReference into a string.
//
// This function assumes that the provided values have already been validated.
func StringSubjectRef(ref *v1.SubjectReference) string {
	if ref.OptionalRelation == "" {
		return StringObjectRef(ref.Object)
	}
	return JoinRelRef(StringObjectRef(ref.Object), ref.OptionalRelation)
}

// MustStringRelationship converts a v1.Relationship to a string.
func MustStringRelationship(rel *v1.Relationship) string {
	relString, err := StringRelationship(rel)
	if err != nil {
		panic(err)
	}
	return relString
}

// StringRelationship converts a v1.Relationship to a string.
func StringRelationship(rel *v1.Relationship) (string, error) {
	if rel == nil || rel.Resource == nil || rel.Subject == nil {
		return "", nil
	}

	caveatString, err := StringCaveatRef(rel.OptionalCaveat)
	if err != nil {
		return "", err
	}

	return StringRelationshipWithoutCaveat(rel) + caveatString, nil
}

// StringRelationshipWithoutCaveat converts a v1.Relationship to a string, excluding any caveat.
func StringRelationshipWithoutCaveat(rel *v1.Relationship) string {
	if rel == nil || rel.Resource == nil || rel.Subject == nil {
		return ""
	}

	return StringObjectRef(rel.Resource) + "#" + rel.Relation + "@" + StringSubjectRef(rel.Subject)
}

// StringCaveatRef converts a v1.ContextualizedCaveat to a string.
func StringCaveatRef(caveat *v1.ContextualizedCaveat) (string, error) {
	if caveat == nil || caveat.CaveatName == "" {
		return "", nil
	}

	contextString, err := StringCaveatContext(caveat.Context)
	if err != nil {
		return "", err
	}

	if len(contextString) > 0 {
		contextString = ":" + contextString
	}

	return "[" + caveat.CaveatName + contextString + "]", nil
}
