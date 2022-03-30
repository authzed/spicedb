package migrations

import (
	"fmt"
)

func createDeletedTransactionsIndex(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_deleted_transaction ON %s (deleted_transaction)",
		mysql.tableTuple())
}

func init() {
	mustRegisterMigration("indexed_gc", "indexes",
		newExecutor(createDeletedTransactionsIndex).migrate,
	)
}
