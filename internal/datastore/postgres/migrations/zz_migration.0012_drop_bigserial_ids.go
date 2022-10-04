package migrations

var dropIDStmts = []string{
	`ALTER TABLE relation_tuple_transaction
		DROP COLUMN id;
`,
	`ALTER TABLE namespace_config
		DROP COLUMN id,
		DROP COLUMN created_transaction,
		DROP COLUMN deleted_transaction;
`,
	`ALTER TABLE relation_tuple
		DROP COLUMN id,
		DROP COLUMN created_transaction,
		DROP COLUMN deleted_transaction;
`,
}

func init() {
	m := &PostgresMigration{
		version:         "drop-bigserial-ids",
		replaces:        "add-xid-constraints",
		expected:        "add-xid-constraints",
		migrationType:   DDL,
		migrationSafety: contract,
	}
	m.Begin()
	for _, stmt := range dropIDStmts {
		m.Statement(stmt)
	}
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)
}
