// The contents of this file are hand-written to add HandwrittenValidate to select message types

package v1

func (m *CheckPermissionRequest) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetResource() != nil && m.GetResource().GetObjectId() == "*" {
		return ObjectReferenceValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return m.GetSubject().HandwrittenValidate()
}

func (m *ExpandPermissionTreeRequest) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetResource() != nil && m.GetResource().GetObjectId() == "*" {
		return ObjectReferenceValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}

func (m *Precondition) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	return m.GetFilter().HandwrittenValidate()
}

func (m *RelationshipFilter) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetOptionalResourceId() == "*" {
		return RelationshipFilterValidationError{
			field:  "OptionalResourceId",
			reason: "alphanumeric value is required",
		}
	}

	return m.GetOptionalSubjectFilter().HandwrittenValidate()
}

func (m *SubjectFilter) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetOptionalSubjectId() == "*" && m.GetOptionalRelation() != nil && m.GetOptionalRelation().GetRelation() != "" {
		return SubjectFilterValidationError{
			field:  "OptionalRelation",
			reason: "optionalrelation must be empty on subject if object ID is a wildcard",
		}
	}
	return nil
}

func (m *RelationshipUpdate) HandwrittenValidate() error {
	return m.GetRelationship().HandwrittenValidate()
}

func (m *SubjectReference) HandwrittenValidate() error {
	if m.GetObject() != nil && m.GetObject().GetObjectId() == "*" && m.GetOptionalRelation() != "" {
		return SubjectReferenceValidationError{
			field:  "OptionalRelation",
			reason: "optionalrelation must be empty on subject if object ID is a wildcard",
		}
	}
	return nil
}

func (m *Relationship) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetResource() != nil && m.GetResource().GetObjectId() == "*" {
		return ObjectReferenceValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return m.GetSubject().HandwrittenValidate()
}

func (m *DeleteRelationshipsRequest) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetOptionalPreconditions() != nil {
		for _, precondition := range m.GetOptionalPreconditions() {
			err := precondition.HandwrittenValidate()
			if err != nil {
				return err
			}
		}
	}

	return m.GetRelationshipFilter().HandwrittenValidate()
}

func (m *WriteRelationshipsRequest) HandwrittenValidate() error {
	if m == nil {
		return nil
	}

	if m.GetOptionalPreconditions() != nil {
		for _, precondition := range m.GetOptionalPreconditions() {
			err := precondition.HandwrittenValidate()
			if err != nil {
				return err
			}
		}
	}

	if m.GetUpdates() != nil {
		for _, update := range m.GetUpdates() {
			if update.GetRelationship() != nil {
				err := update.GetRelationship().HandwrittenValidate()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
