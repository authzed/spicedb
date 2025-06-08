package migrations

import "fmt"

func convertNamespaceDefinitionToLongBlob(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s
		MODIFY serialized_config LONGBLOB NOT NULL;`,
		t.Namespace(),
	)
}

func convertCaveatDefinitionToLongBlob(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s
		MODIFY definition LONGBLOB NOT NULL;`,
		t.Caveat(),
	)
}

func init() {
	mustRegisterMigration("longblob_definitions", "extend_object_id", noNonatomicMigration,
		newStatementBatch(
			convertNamespaceDefinitionToLongBlob,
			convertCaveatDefinitionToLongBlob,
		).execute,
	)
}
