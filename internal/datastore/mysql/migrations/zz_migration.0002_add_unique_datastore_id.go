package migrations

import (
	"fmt"
)

func createMetadataTable(driver *MySQLDriver) string {
	return fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
		unique_id VARCHAR(36));`,
		driver.Metadata(),
	)
}

func init() {
	mustRegisterMigration("add_unique_datastore_id", "initial",
		newExecutor(
			createMetadataTable,
		).migrate,
	)
}
