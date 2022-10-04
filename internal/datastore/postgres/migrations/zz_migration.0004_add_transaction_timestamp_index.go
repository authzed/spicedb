package migrations

const createIndexOnTupleTransactionTimestamp = `CREATE INDEX ix_relation_tuple_transaction_by_timestamp on relation_tuple_transaction(timestamp);
`

func init() {
	m := &PostgresMigration{
		version:         "add-transaction-timestamp-index",
		replaces:        "add-unique-living-ns",
		expected:        "add-unique-living-ns",
		migrationType:   DDL,
		migrationSafety: expand,
	}
	m.Begin()
	m.Statement(createIndexOnTupleTransactionTimestamp)
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)
}
