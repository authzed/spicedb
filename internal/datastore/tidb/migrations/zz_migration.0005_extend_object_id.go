package migrations

import "fmt"

// Extend the object ID column to 1024 characters
func adjustObjectIDLength(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s
		CONVERT TO CHARACTER SET latin1,
		MODIFY object_id VARCHAR(1024) NOT NULL,
		MODIFY userset_object_id VARCHAR(1024) NOT NULL;`,
		t.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("extend_object_id", "add_caveat", noNonatomicMigration,
		newStatementBatch(
			adjustObjectIDLength,
		).execute,
	)
}
