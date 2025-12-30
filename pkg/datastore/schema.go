package datastore

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// RevisionedTypeDefinition is a revisioned version of a type definition.
type RevisionedTypeDefinition = RevisionedDefinition[*core.NamespaceDefinition]

// SchemaReadable is an interface for datastores that support reading schema information.
type SchemaReadable interface {
	// SchemaReader returns a SchemaReader for reading schema information.
	SchemaReader() (SchemaReader, error)
}

// SchemaReader is an interface for reading schema information at a particular revision
// or under a transaction.
type SchemaReader interface {
	// SchemaText returns the schema text at the current revision.
	SchemaText() (string, error)

	// LookupTypeDefByName looks up a type definition by name. Returns the definition and true if found,
	// or an empty definition and false if not found.
	LookupTypeDefByName(ctx context.Context, name string) (RevisionedTypeDefinition, bool, error)

	// LookupCaveatDefByName looks up a caveat definition by name. Returns the definition and true if found,
	// or an empty definition and false if not found.
	LookupCaveatDefByName(ctx context.Context, name string) (RevisionedCaveat, bool, error)

	// ListAllTypeDefinitions lists all type definitions (namespaces).
	ListAllTypeDefinitions(ctx context.Context) ([]RevisionedTypeDefinition, error)

	// ListAllCaveatDefinitions lists all caveat definitions.
	ListAllCaveatDefinitions(ctx context.Context) ([]RevisionedCaveat, error)

	// ListAllSchemaDefinitions lists all type and caveat definitions.
	ListAllSchemaDefinitions(ctx context.Context) (map[string]SchemaDefinition, error)

	// LookupSchemaDefinitionsByNames looks up type and caveat definitions by name.
	LookupSchemaDefinitionsByNames(ctx context.Context, names []string) (map[string]SchemaDefinition, error)
}
