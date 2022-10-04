package migrations

import "fmt"

const dropNSConfigIDPkey = `ALTER TABLE namespace_config DROP CONSTRAINT namespace_config_pkey;
`

var addXIDConstraints = []string{
	`ALTER TABLE relation_tuple_transaction
		DROP CONSTRAINT pk_rttx,
		ADD CONSTRAINT pk_rttx PRIMARY KEY USING INDEX ix_rttx_pk;`,
	`ALTER TABLE namespace_config
		ADD CONSTRAINT pk_namespace_config PRIMARY KEY USING INDEX ix_namespace_config_pk,
		ADD CONSTRAINT uq_namespace_living_xid UNIQUE USING INDEX ix_namespace_config_living;`,
	`ALTER TABLE relation_tuple
		DROP CONSTRAINT pk_relation_tuple,
		ADD CONSTRAINT pk_relation_tuple PRIMARY KEY USING INDEX ix_relation_tuple_pk,
		ADD CONSTRAINT uq_relation_tuple_living_xid UNIQUE USING INDEX ix_relation_tuple_living;`,
}

func init() {
	m := &PostgresMigration{
		version:         "drop-ns-config-pk-2",
		replaces:        "add-xid-indices",
		expected:        "add-xid-indices",
		migrationType:   DDL,
		migrationSafety: contract,
	}
	m.Begin()
	m.Statement(dropNSConfigIDPkey)
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)

	previous := m.version
	for i, stmt := range addXIDConstraints {
		m := &PostgresMigration{
			version:         fmt.Sprintf("add-xid-constraints-%d", i),
			replaces:        previous,
			expected:        m.version,
			migrationType:   DDL,
			migrationSafety: contract,
		}
		m.Statement(stmt)
		RegisterPGMigration(m)
		previous = m.version
	}
	m2 := &PostgresMigration{
		version:         "add-xid-constraints",
		replaces:        previous,
		expected:        m.version,
		migrationType:   DML,
		migrationSafety: expand,
	}
	// the previous migrations don't write the version, so check n-1
	m2.WriteVersionOverride(m.version)
	RegisterPGMigration(m2)
}
