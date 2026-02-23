package datastore

import (
	"context"
	"testing"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
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

	// LookupTypeDefinitionsByNames looks up type definitions by name.
	LookupTypeDefinitionsByNames(ctx context.Context, names []string) (map[string]SchemaDefinition, error)

	// LookupCaveatDefinitionsByNames looks up caveat definitions by name.
	LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]SchemaDefinition, error)
}

// SchemaWriteable is an interface for datastores that support writing schema information.
type SchemaWriteable interface {
	// SchemaWriter returns a SchemaWriter for writing schema information.
	SchemaWriter() (SchemaWriter, error)
}

// SchemaWriter is an interface for writing schema information at a particular revision
// or under a transaction.
type SchemaWriter interface {
	// WriteSchema writes the full set of schema definitions. The schema string is provided for
	// future use but is currently ignored by implementations. The method validates that no
	// definition names overlap, loads existing definitions, replaces changed ones, and deletes
	// definitions no longer present.
	WriteSchema(ctx context.Context, definitions []SchemaDefinition, schemaString string, caveatTypeSet *caveattypes.TypeSet) error

	// AddDefinitionsForTesting adds or overwrites the given schema definitions. This method is
	// only for use in testing and requires a testing.TB instance to enforce this constraint.
	AddDefinitionsForTesting(ctx context.Context, tb testing.TB, definitions ...SchemaDefinition) error
}
