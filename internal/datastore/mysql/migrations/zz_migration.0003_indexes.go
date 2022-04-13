package migrations

import (
	"fmt"
)

func createReverseQueryIndex(driver *MySQLDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject ON %s (userset_object_id, userset_namespace, userset_relation, namespace, relation)",
		driver.RelationTuple(),
	)
}

func createReverseCheckIndex(driver *MySQLDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject_relation ON %s (userset_namespace, userset_relation, namespace, relation)",
		driver.RelationTuple(),
	)
}

func createIndexOnTupleTransactionTimestamp(driver *MySQLDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_transaction_by_timestamp on %s (timestamp)",
		driver.RelationTupleTransaction(),
	)
}

func init() {
	mustRegisterMigration("indexes", "namespace_tables",
		newExecutor(
			createReverseQueryIndex,
			createReverseCheckIndex,
			createIndexOnTupleTransactionTimestamp,
		).migrate,
	)
}
