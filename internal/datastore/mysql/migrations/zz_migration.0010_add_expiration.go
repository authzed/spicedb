package migrations

import "fmt"

func addExpirationToRelationTupleTable(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s
		ADD COLUMN expiration DATETIME NULL DEFAULT NULL;`,
		t.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("add_expiration_to_relation_tuple", "add_metadata_to_transaction_table", noNonatomicMigration,
		newStatementBatch(
			addExpirationToRelationTupleTable,
		).execute,
	)
}
