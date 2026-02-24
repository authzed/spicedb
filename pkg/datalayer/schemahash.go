package datalayer

// SchemaHash is a string that uniquely identifies a specific version of a schema.
// It is used by caching layers to avoid redundant reads of the schema from the
// underlying datastore when the schema hasn't changed.
type SchemaHash string

const (
	// NoSchemaHashInTransaction is a sentinel value used when reading within a
	// read-write transaction where the schema revision is not yet stable.
	NoSchemaHashInTransaction SchemaHash = "no-schema-hash-in-transaction"

	// NoSchemaHashInDevelopment is a sentinel value used when operating in
	// development mode, where caching of schema is not desired.
	NoSchemaHashInDevelopment SchemaHash = "no-schema-hash-in-development"

	// NoSchemaHashForTesting is a sentinel value used in tests, where schema
	// caching is not needed.
	NoSchemaHashForTesting SchemaHash = "no-schema-hash-for-testing"

	// NoSchemaHashForWatch is a sentinel value used when reading schema for
	// watch operations, where the hash is not yet available.
	NoSchemaHashForWatch SchemaHash = "no-schema-hash-for-watch"

	// NoSchemaHashForLegacyCursor is a sentinel value for decoding legacy cursors
	// that do not contain a schema hash field.
	NoSchemaHashForLegacyCursor SchemaHash = "no-schema-hash-for-legacy-cursor"

	// NoSchemaHashInLegacyMode is a sentinel value used when the DataLayer is
	// operating in legacy schema mode, where no unified schema exists.
	NoSchemaHashInLegacyMode SchemaHash = "no-schema-hash-in-legacy-mode"
)

// IsBypassSentinel returns true if this SchemaHash is a sentinel value that
// should bypass any caching.
func (sh SchemaHash) IsBypassSentinel() bool {
	switch sh {
	case NoSchemaHashInTransaction,
		NoSchemaHashInDevelopment,
		NoSchemaHashForTesting,
		NoSchemaHashForWatch,
		NoSchemaHashForLegacyCursor,
		NoSchemaHashInLegacyMode:
		return true
	default:
		return false
	}
}
