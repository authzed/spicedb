package migrations

const (
	dropNSConfigPK = `ALTER TABLE namespace_config DROP CONSTRAINT IF EXISTS pk_namespace_config;
`
	createNSConfigID = `ALTER TABLE namespace_config ADD COLUMN id BIGSERIAL PRIMARY KEY;
`
)

func init() {
	m := &PostgresMigration{
		version:         "add-ns-config-id",
		replaces:        "add-unique-datastore-id",
		expected:        "add-unique-datastore-id",
		migrationType:   DDL,
		migrationSafety: expand,
	}
	m.Begin()
	m.Statement(dropNSConfigPK)
	m.Statement(createNSConfigID)
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)
}
