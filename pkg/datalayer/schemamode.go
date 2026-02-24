package datalayer

import "fmt"

// SchemaMode represents the experimental schema mode for datastore operations.
// It controls how schema is read from and written to the datastore, allowing
// a gradual migration from legacy per-definition storage to unified schema storage.
type SchemaMode uint8

const (
	// SchemaModeReadLegacyWriteLegacy uses legacy schema reader and writer.
	// This is the default and backward-compatible mode.
	SchemaModeReadLegacyWriteLegacy SchemaMode = iota

	// SchemaModeReadLegacyWriteBoth uses legacy schema reader and writes to both
	// legacy and unified schema storage. Use this as the first migration step.
	SchemaModeReadLegacyWriteBoth

	// SchemaModeReadNewWriteBoth uses unified schema reader and writes to both
	// legacy and unified schema storage. Use this as the second migration step.
	SchemaModeReadNewWriteBoth

	// SchemaModeReadNewWriteNew uses unified schema reader and writer only.
	// This is the final migration target.
	SchemaModeReadNewWriteNew
)

var schemaModeNames = map[string]SchemaMode{
	"read-legacy-write-legacy": SchemaModeReadLegacyWriteLegacy,
	"read-legacy-write-both":   SchemaModeReadLegacyWriteBoth,
	"read-new-write-both":      SchemaModeReadNewWriteBoth,
	"read-new-write-new":       SchemaModeReadNewWriteNew,
}

var schemaModeStrings = map[SchemaMode]string{
	SchemaModeReadLegacyWriteLegacy: "read-legacy-write-legacy",
	SchemaModeReadLegacyWriteBoth:   "read-legacy-write-both",
	SchemaModeReadNewWriteBoth:      "read-new-write-both",
	SchemaModeReadNewWriteNew:       "read-new-write-new",
}

// ParseSchemaMode converts a string to a SchemaMode. Returns an error if the string is invalid.
func ParseSchemaMode(s string) (SchemaMode, error) {
	mode, ok := schemaModeNames[s]
	if !ok {
		return SchemaModeReadLegacyWriteLegacy, fmt.Errorf(
			"invalid schema mode %q, must be one of: read-legacy-write-legacy, read-legacy-write-both, read-new-write-both, read-new-write-new", s)
	}
	return mode, nil
}

// String returns the string representation of the SchemaMode.
func (s SchemaMode) String() string {
	str, ok := schemaModeStrings[s]
	if !ok {
		return "unknown"
	}
	return str
}

// ReadsFromNew returns true if the mode reads from the unified schema storage.
func (s SchemaMode) ReadsFromNew() bool {
	return s == SchemaModeReadNewWriteBoth || s == SchemaModeReadNewWriteNew
}

// WritesToLegacy returns true if the mode writes to legacy schema storage.
func (s SchemaMode) WritesToLegacy() bool {
	return s == SchemaModeReadLegacyWriteLegacy || s == SchemaModeReadLegacyWriteBoth || s == SchemaModeReadNewWriteBoth
}

// WritesToNew returns true if the mode writes to unified schema storage.
func (s SchemaMode) WritesToNew() bool {
	return s == SchemaModeReadLegacyWriteBoth || s == SchemaModeReadNewWriteBoth || s == SchemaModeReadNewWriteNew
}
