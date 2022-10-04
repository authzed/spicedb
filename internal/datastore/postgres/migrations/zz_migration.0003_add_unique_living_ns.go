package migrations

const createUniqueLivingNamespaceConstraint = `ALTER TABLE namespace_config
	ADD CONSTRAINT uq_namespace_living UNIQUE (namespace, deleted_transaction);
`

const deleteAllButNewestNamespace = `DELETE FROM namespace_config WHERE namespace IN (
		SELECT namespace FROM namespace_config WHERE deleted_transaction = 9223372036854775807 GROUP BY namespace HAVING COUNT(created_transaction) > 1
	) AND (namespace, created_transaction) NOT IN (
		SELECT namespace, max(created_transaction) from namespace_config where deleted_transaction = 9223372036854775807 GROUP BY namespace HAVING COUNT(created_transaction) > 1);
`

func init() {
	m := &PostgresMigration{
		version:         "delete-all-but-newest-ns",
		replaces:        "add-reverse-index",
		expected:        "add-reverse-index",
		migrationType:   DML,
		migrationSafety: contract,
	}
	m.Begin()
	m.Statement(deleteAllButNewestNamespace)
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)

	m2 := &PostgresMigration{
		version:         "add-unique-living-ns",
		replaces:        "delete-all-but-newest-ns",
		expected:        "delete-all-but-newest-ns",
		migrationType:   DDL,
		migrationSafety: expand,
	}
	m2.Begin()
	m2.Statement(createUniqueLivingNamespaceConstraint)
	m2.WriteVersion()
	m2.Commit()
	RegisterPGMigration(m2)
}
