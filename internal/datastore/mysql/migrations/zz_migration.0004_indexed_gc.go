package migrations

import (
	"fmt"
)

func createDeletedTransactionsIndex(driver *MySQLDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_deleted_transaction ON %s (deleted_transaction)",
		driver.RelationTuple(),
	)
}

func init() {
	mustRegisterMigration("indexed_gc", "indexes",
		newExecutor(createDeletedTransactionsIndex).migrate,
	)
}
