package datastore

import (
	"context"
	"time"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// SchemaWatchCallback is invoked when the schema hash changes.
// The callback receives the new schema hash and the revision at which it was observed.
type SchemaWatchCallback func(schemaHash string, revision Revision) error

// SingleStoreSchemaHashWatcher defines methods for watching schema hash changes in the unified single-store schema hash table.
type SingleStoreSchemaHashWatcher interface {
	// WatchSchemaHash watches for changes to the schema hash and invokes the callback when it changes.
	// The refreshInterval specifies how often to check for changes (for polling implementations).
	// For watch-based implementations (CRDB, Spanner), this may be used as a hint for reconnection timing.
	// The watch continues until the context is canceled.
	// Returns an error if the watch cannot be started.
	WatchSchemaHash(ctx context.Context, refreshInterval time.Duration, callback SchemaWatchCallback) error
}

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
