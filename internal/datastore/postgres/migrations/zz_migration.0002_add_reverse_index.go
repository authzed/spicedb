package migrations

const (
	createReverseQueryIndex = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation);
`
	createReverseCheckIndex = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (userset_namespace, userset_relation, namespace, relation);
`
)

func init() {
	m := &PostgresMigration{
		version:         "add-reverse-index",
		replaces:        "1eaeba4b8a73",
		expected:        "1eaeba4b8a73",
		migrationType:   DDL,
		migrationSafety: expand,
	}
	m.Begin()
	m.Statement(createReverseQueryIndex)
	m.Statement(createReverseCheckIndex)
	m.WriteVersion()
	m.Commit()
	RegisterPGMigration(m)
}
