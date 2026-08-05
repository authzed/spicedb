package datalayer

import "github.com/authzed/spicedb/pkg/datalayer/schemamode"

// The schema mode type lives in the leaf package pkg/datalayer/schemamode so
// that configuration packages, such as pkg/cmd/datastore/dsconfig, can name it
// without depending on pkg/datalayer. It is aliased here so callers can keep
// using datalayer.SchemaMode.

// SchemaMode represents the experimental schema mode for datastore operations.
// It controls how schema is read from and written to the datastore, allowing
// a gradual migration from legacy per-definition storage to unified schema storage.
type SchemaMode = schemamode.SchemaMode

const (
	// SchemaModeReadLegacyWriteLegacy uses legacy schema reader and writer.
	// This is the default and backward-compatible mode.
	SchemaModeReadLegacyWriteLegacy = schemamode.SchemaModeReadLegacyWriteLegacy

	// SchemaModeReadLegacyWriteBoth uses legacy schema reader and writes to both
	// legacy and unified schema storage. Use this as the first migration step.
	SchemaModeReadLegacyWriteBoth = schemamode.SchemaModeReadLegacyWriteBoth

	// SchemaModeReadNewWriteBoth uses unified schema reader and writes to both
	// legacy and unified schema storage. Use this as the second migration step.
	SchemaModeReadNewWriteBoth = schemamode.SchemaModeReadNewWriteBoth

	// SchemaModeReadNewWriteNew uses unified schema reader and writer only.
	// This is the final migration target.
	SchemaModeReadNewWriteNew = schemamode.SchemaModeReadNewWriteNew
)

// ParseSchemaMode converts a string to a SchemaMode. Returns an error if the string is invalid.
func ParseSchemaMode(s string) (SchemaMode, error) {
	return schemamode.ParseSchemaMode(s)
}
