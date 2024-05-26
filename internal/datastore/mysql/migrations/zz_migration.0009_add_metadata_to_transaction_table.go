package migrations

import "fmt"

func addMetadataToTransactionTable(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s
		ADD COLUMN metadata BLOB NULL DEFAULT NULL;`,
		t.RelationTupleTransaction(),
	)
}

func init() {
	mustRegisterMigration("add_metadata_to_transaction_table", "add_relationship_counters_table", noNonatomicMigration,
		newStatementBatch(
			addMetadataToTransactionTable,
		).execute,
	)
}
