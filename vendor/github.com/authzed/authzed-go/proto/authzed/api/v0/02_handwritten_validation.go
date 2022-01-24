// The contents of this file are hand-written to add HandwrittenValidate to select message types

package v0

func (m *CheckRequest) HandwrittenValidate() error {
	if m.GetTestUserset() != nil && m.GetTestUserset().GetObjectId() == "*" {
		return ObjectAndRelationValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}

func (m *ContentChangeCheckRequest) HandwrittenValidate() error {
	if m.GetTestUserset() != nil && m.GetTestUserset().GetObjectId() == "*" {
		return ObjectAndRelationValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}

func (m *ExpandRequest) HandwrittenValidate() error {
	if m.GetUserset() != nil && m.GetUserset().GetObjectId() == "*" {
		return ObjectAndRelationValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}

func (m *LookupRequest) HandwrittenValidate() error {
	if m.GetUser() != nil && m.GetUser().GetObjectId() == "*" {
		return ObjectAndRelationValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}

func (m *RelationTuple) HandwrittenValidate() error {
	if m.GetObjectAndRelation() != nil && m.GetObjectAndRelation().GetObjectId() == "*" {
		return ObjectAndRelationValidationError{
			field:  "ObjectId",
			reason: "alphanumeric value is required",
		}
	}

	return nil
}

func (m *RelationTupleUpdate) HandwrittenValidate() error {
	if m.GetTuple() != nil {
		return m.GetTuple().HandwrittenValidate()
	}

	return nil
}

func (m *WriteRequest) HandwrittenValidate() error {
	if m.GetWriteConditions() != nil {
		for _, condition := range m.GetWriteConditions() {
			err := condition.HandwrittenValidate()
			if err != nil {
				return err
			}
		}
	}

	if m.GetUpdates() != nil {
		for _, update := range m.GetUpdates() {
			err := update.HandwrittenValidate()
			if err != nil {
				return err
			}
		}
	}

	return nil
}
