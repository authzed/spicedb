package migrations

import (
	"fmt"
)

var addXIDIndices = []string{
	// Replace the indices that are inherent from having a primary key constraint
	"CREATE UNIQUE INDEX CONCURRENTLY ix_rttx_oldpk ON relation_tuple_transaction (id)",
	"CREATE UNIQUE INDEX CONCURRENTLY ix_namespace_config_oldpk ON namespace_config (id)",

	// Add indices that will eventually back our new constraints
	"CREATE UNIQUE INDEX CONCURRENTLY ix_rttx_pk ON relation_tuple_transaction (xid);",
	`CREATE UNIQUE INDEX CONCURRENTLY ix_namespace_config_pk
		ON namespace_config (namespace, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY ix_namespace_config_living
		ON namespace_config (namespace, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY ix_relation_tuple_pk
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
						   userset_relation, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY ix_relation_tuple_living
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
						   userset_relation, deleted_xid);`,
}

func init() {
	previous := "add-xid-columns"
	for i, stmt := range addXIDIndices {
		m := &PostgresMigration{
			version:         fmt.Sprintf("add-xid-indices-%d", i),
			replaces:        previous,
			expected:        "add-xid-columns",
			migrationType:   DDL,
			migrationSafety: contract,
		}
		m.Statement(stmt)
		RegisterPGMigration(m)
		previous = m.version
	}

	m2 := &PostgresMigration{
		version:         "add-xid-indices",
		replaces:        previous,
		expected:        "add-xid-columns",
		migrationType:   DML,
		migrationSafety: expand,
	}
	// the previous migrations don't write the version, so check n-1
	m2.WriteVersionOverride("add-xid-columns")
	RegisterPGMigration(m2)
}
