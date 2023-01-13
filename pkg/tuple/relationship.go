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
