package commonschemadsl

import (
	"google.golang.org/protobuf/proto"
)
// SchemaDefinition represents an object or caveat definition in a schema.
type SchemaDefinition interface {
	proto.Message

	GetName() string
}
