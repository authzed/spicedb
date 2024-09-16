package tuple

func Create(rel Relationship) RelationshipUpdate {
	return RelationshipUpdate{
		Operation:    UpdateOperationCreate,
		Relationship: rel,
	}
}

func Touch(rel Relationship) RelationshipUpdate {
	return RelationshipUpdate{
		Operation:    UpdateOperationTouch,
		Relationship: rel,
	}
}

func Delete(rel Relationship) RelationshipUpdate {
	return RelationshipUpdate{
		Operation:    UpdateOperationDelete,
		Relationship: rel,
	}
}
