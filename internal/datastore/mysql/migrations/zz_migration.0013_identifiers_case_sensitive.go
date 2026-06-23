package migrations

import "fmt"

// SpiceDB identifiers are case-SENSITIVE, matching the Postgres, CockroachDB, and Spanner
// datastores. On MySQL every table was created with the utf8mb4 default collation
// (utf8mb4_0900_ai_ci), which is case-INSENSITIVE, and relation_tuple was later moved to latin1
// (latin1_swedish_ci, also case-insensitive). Both incorrectly collide identifiers that differ
// only in letter case (e.g. "Foo" and "foo") in the unique indexes and in lookups.
//
// Two identifier kinds are reachable with mixed case via the API and are the real fixes here:
//   - object IDs / subject IDs (regex allows [a-zA-Z0-9...]) -> relation_tuple
//   - relationship counter names (no regex, only non-empty)  -> relationship_counter.name
// Object types, relation names, and caveat names are constrained to lowercase by protovalidate
// (^[a-z]...$), so they can never collide on case; converting their columns is defense-in-depth
// and consistency only.
//
// relation_tuple is converted to latin1_bin: its object_id/userset_object_id columns are
// VARCHAR(1024), which at utf8mb4 (4 bytes/char) would exceed InnoDB's 3072-byte index key limit,
// so latin1 (1 byte/char) is retained. CONVERT TO leaves the JSON caveat_context and BLOB columns
// unaffected (see migration 0005, which already CONVERT TO'd this table to latin1).
//
// The remaining tables keep utf8mb4 and only their name/identifier columns move to utf8mb4_bin
// (binary, case-sensitive); every such column is <= 700 chars and fits under the 3072-byte key
// limit. SpiceDB never JOINs string columns across tables, so the mixed latin1_bin/utf8mb4_bin
// collations cannot produce "illegal mix of collations" errors.
//
// Converting a column from a case-insensitive to a case-sensitive collation can only relax
// uniqueness (more distinct values), so it cannot violate any existing unique constraint.

func makeRelationTupleCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s CONVERT TO CHARACTER SET latin1 COLLATE latin1_bin;`,
		t.RelationTuple(),
	)
}

func makeNamespaceConfigCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s MODIFY namespace VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;`,
		t.Namespace(),
	)
}

func makeCaveatCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s MODIFY name VARCHAR(700) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;`,
		t.Caveat(),
	)
}

func makeRelationshipCounterCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s MODIFY name VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;`,
		t.RelationshipCounters(),
	)
}

func makeStoredSchemaCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s MODIFY name VARCHAR(700) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;`,
		t.Schema(),
	)
}

func makeStoredSchemaRevisionCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s MODIFY name VARCHAR(700) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'current';`,
		t.SchemaRevision(),
	)
}

// makeMetadataUniqueIDCaseSensitive converts the datastore's unique_id column. It is a UUID and
// not part of any index, so this is consistency only; the column is nullable, which is preserved.
// (The migration-version tracking table is intentionally not touched: its column is framework
// metadata, not SpiceDB data.)
func makeMetadataUniqueIDCaseSensitive(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s MODIFY unique_id VARCHAR(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;`,
		t.Metadata(),
	)
}

func init() {
	mustRegisterMigration(
		"identifiers_case_sensitive", "populate_schema_tables", noNonatomicMigration,
		newStatementBatch(
			makeRelationTupleCaseSensitive,
			makeNamespaceConfigCaseSensitive,
			makeCaveatCaseSensitive,
			makeRelationshipCounterCaseSensitive,
			makeStoredSchemaCaseSensitive,
			makeStoredSchemaRevisionCaseSensitive,
			makeMetadataUniqueIDCaseSensitive,
		).execute,
	)
}
