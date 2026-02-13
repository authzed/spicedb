package datastore

import (
	"context"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// SingleStoreSchemaReader defines methods for reading schema from the unified single-store schema table.
type SingleStoreSchemaReader interface {
	// ReadStoredSchema reads the stored schema from the unified schema table.
	// Returns ErrSchemaNotFound if no schema has been written.
	ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error)
}

// SingleStoreSchemaWriter defines methods for writing schema to the unified single-store schema table.
type SingleStoreSchemaWriter interface {
	// WriteStoredSchema writes the stored schema to the unified schema table.
	WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error
}

// LegacySchemaHashWriter defines an optional method for writing just the schema hash.
// This is used by datastores to write the schema hash during legacy schema writes
// without reading back buffered writes.
type LegacySchemaHashWriter interface {
	// WriteLegacySchemaHashFromDefinitions writes the schema hash computed from the given definitions.
	// This is called by the legacy schema adapter after buffering writes but before commit.
	WriteLegacySchemaHashFromDefinitions(ctx context.Context, namespaces []RevisionedNamespace, caveats []RevisionedCaveat) error
}

// DualSchemaReader combines both legacy and single-store schema reading interfaces.
// Datastores should implement this interface to support both schema storage modes.
type DualSchemaReader interface {
	LegacySchemaReader
	SingleStoreSchemaReader
}

// DualSchemaWriter combines both legacy and single-store schema writing interfaces.
// Datastores should implement this interface to support both schema storage modes.
type DualSchemaWriter interface {
	LegacySchemaWriter
	SingleStoreSchemaWriter
}
