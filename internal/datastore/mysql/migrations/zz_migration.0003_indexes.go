package migrations

import (
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
)

var (
	createReverseQueryIndex                = fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject ON %s (userset_object_id, userset_namespace, userset_relation, namespace, relation)", common.TableTuple)
	createReverseCheckIndex                = fmt.Sprintf("CREATE INDEX ix_relation_tuple_by_subject_relation ON %s (userset_namespace, userset_relation, namespace, relation)", common.TableTuple)
	createIndexOnTupleTransactionTimestamp = fmt.Sprintf("CREATE INDEX ix_relation_tuple_transaction_by_timestamp on %s (timestamp)", common.TableTransaction)
)

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
