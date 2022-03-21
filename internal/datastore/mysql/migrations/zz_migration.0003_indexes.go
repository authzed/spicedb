package migrations

import (
	"fmt"
)

func createReverseQueryIndex(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject ON %s (userset_object_id, userset_namespace, userset_relation, namespace, relation)",
		mysql.tableTuple())
}

func createReverseCheckIndex(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject_relation ON %s (userset_namespace, userset_relation, namespace, relation)",
		mysql.tableTuple())
}

func createIndexOnTupleTransactionTimestamp(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_transaction_by_timestamp on %s (timestamp)",
		mysql.tableTransaction())
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
