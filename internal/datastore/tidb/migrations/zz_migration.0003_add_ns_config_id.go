package migrations

import "fmt"

func dropNSConfigPK(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s DROP PRIMARY KEY;`,
		t.tableNamespace,
	)
}

func createNSConfigID(t *tables) string {
	return fmt.Sprintf(
		`ALTER TABLE %s ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;`,
		t.tableNamespace,
	)
}

func init() {
	mustRegisterMigration("add_ns_config_id", "add_unique_datastore_id", noNonatomicMigration,
		newStatementBatch(
			dropNSConfigPK,
			createNSConfigID,
		).execute,
	)
}
