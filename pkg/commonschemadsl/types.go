package commonschemadsl

import (
	"google.golang.org/protobuf/proto"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)
// SchemaDefinition represents an object or caveat definition in a schema.
type SchemaDefinition interface {
	proto.Message

	GetName() string
}

// CompiledSchema is the result of compiling a schema when there are no errors.
type CompiledSchema interface {
	// ObjectDefinitions holds the object definitions in the schema.
	GetObjectDefinitions() []*core.NamespaceDefinition

	// CaveatDefinitions holds the caveat definitions in the schema.
	GetCaveatDefinitions() []*core.CaveatDefinition

	// OrderedDefinitions holds the object and caveat definitions in the schema, in the
	// order in which they were found.
	GetOrderedDefinitions() []SchemaDefinition
}
