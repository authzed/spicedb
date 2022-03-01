package migrations

import (
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
)

func createReverseQueryIndex(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject ON %s (userset_object_id, userset_namespace, userset_relation, namespace, relation)",
		mysql.TablePrefix+common.TableTupleDefault)
}

func createReverseCheckIndex(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject_relation ON %s (userset_namespace, userset_relation, namespace, relation)",
		mysql.TablePrefix+common.TableTupleDefault)
}

func createIndexOnTupleTransactionTimestamp(mysql *MysqlDriver) string {
	return fmt.Sprintf("CREATE INDEX ix_relation_tuple_transaction_by_timestamp on %s (timestamp)",
		mysql.TablePrefix+common.TableTransactionDefault)
}

func init() {
	err := Manager.Register("indexes", "namespace-tables",
		newExecutor(
			createReverseQueryIndex,
			createReverseCheckIndex,
			createIndexOnTupleTransactionTimestamp,
		).migrate,
	)
	if err != nil {
		panic("failed to register migration  " + err.Error())
	}
}
