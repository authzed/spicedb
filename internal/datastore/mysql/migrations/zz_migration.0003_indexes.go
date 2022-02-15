package migrations

const (
	createReverseQueryIndex                = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`
	createReverseCheckIndex                = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (userset_namespace, userset_relation, namespace, relation)`
	createIndexOnTupleTransactionTimestamp = `CREATE INDEX ix_relation_tuple_transaction_by_timestamp on relation_tuple_transaction(timestamp)`
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
