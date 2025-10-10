package schema

// CloneSchema creates a deep copy of a Schema and all its nested structures.
// All parent references are properly maintained in the cloned structure.
func CloneSchema(s *Schema) *Schema {
	return s.clone()
}
