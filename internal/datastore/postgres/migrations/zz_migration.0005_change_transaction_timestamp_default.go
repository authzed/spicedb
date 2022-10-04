package migrations

const alterTimestampDefaultValue = `ALTER TABLE relation_tuple_transaction 
		ALTER COLUMN timestamp SET DEFAULT (now() AT TIME ZONE 'UTC');
`

func init() {
	m := &PostgresMigration{
		version:         "change-transaction-timestamp-default",
		replaces:        "add-transaction-timestamp-index",
		expected:        "add-transaction-timestamp-index",
		migrationType:   DDL,
		migrationSafety: contract,
	}
	m.Begin()
	m.Statement(alterTimestampDefaultValue)
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)
}
