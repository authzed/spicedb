package datastore

// DefinitionsOf returns just the schema definitions found in the list of revisioned
// definitions.
func DefinitionsOf[T SchemaDefinition](revisionedDefinitions []RevisionedDefinition[T]) []T {
	definitions := make([]T, 0, len(revisionedDefinitions))
	for _, revDef := range revisionedDefinitions {
		definitions = append(definitions, revDef.Definition)
	}
	return definitions
}
