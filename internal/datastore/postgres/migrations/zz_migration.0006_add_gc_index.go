package migrations

const (
	createDeletedTransactionIndex = `CREATE INDEX CONCURRENTLY ix_relation_tuple_by_deleted_transaction ON relation_tuple (deleted_transaction);
`
)

func init() {
	// TODO: make this version unselectable via spicedb migrate
	m := &PostgresMigration{
		version:         "add-gc-index-internal",
		replaces:        "change-transaction-timestamp-default",
		expected:        "change-transaction-timestamp-default",
		migrationType:   DDL,
		migrationSafety: expand,
	}
	// CREATE INDEX CONCURRENTLY can't run in transaction and can't be sent
	// with multiple commands, so this migration skips updating the version.
	m.Statement(createDeletedTransactionIndex)
	RegisterPGMigration(m)

	m2 := &PostgresMigration{
		version:         "add-gc-index",
		replaces:        "add-gc-index-internal",
		expected:        "change-transaction-timestamp-default",
		migrationType:   DML,
		migrationSafety: expand,
	}
	// the previous migration doesn't write the version, so check n-1
	m2.WriteVersionOverride("change-transaction-timestamp-default")
	RegisterPGMigration(m2)
}
