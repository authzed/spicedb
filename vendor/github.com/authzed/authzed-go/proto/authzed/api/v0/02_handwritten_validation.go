// The contents of this file are hand-written to add HandwrittenValidate to select message types

package v0

func (m *RelationTuple) HandwrittenValidate() error {
	if m.GetObjectAndRelation() != nil && m.GetObjectAndRelation().GetObjectId() == "*" {
		return ObjectAndRelationValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}
