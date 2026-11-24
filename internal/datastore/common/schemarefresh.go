package common

// UnifiedSchemaState describes the stored unified schema as a refresh found it.
type UnifiedSchemaState struct {
	// Exists reports that a live unified schema is stored.
	Exists bool

	// Hash is the stored unified schema hash.
	Hash string
}
