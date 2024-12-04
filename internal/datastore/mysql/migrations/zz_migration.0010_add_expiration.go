package migrations

import "fmt"

func addExpirationToRelationTupleTable(t *tables) string {
	return fmt.Sprintf(`ALTER TABLE %s
		ADD COLUMN expiration DATETIME NULL DEFAULT NULL;`,
		t.RelationTuple(),
	)
}

// Used for cleaning up expired relationships.
func addExpiredRelationshipsIndex(t *tables) string {
	return fmt.Sprintf(`CREATE INDEX ix_%s_expired ON %s (expiration);`,
		t.RelationTuple(),
		t.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("add_expiration_to_relation_tuple", "add_metadata_to_transaction_table", noNonatomicMigration,
		newStatementBatch(
			addExpirationToRelationTupleTable,
			addExpiredRelationshipsIndex,
		).execute,
	)
}
