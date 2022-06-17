package migrations

import (
	"fmt"
)

func createMetadataTable(t *tables) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		unique_id VARCHAR(36));`,
		t.Metadata(),
	)
}

func init() {
	mustRegisterMigration("add_unique_datastore_id", "initial", noNonatomicMigration,
		newStatementBatch(
			createMetadataTable,
		).execute,
	)
}
