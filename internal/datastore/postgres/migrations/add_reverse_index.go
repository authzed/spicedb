package migrations

const (
	createReverseQueryIndex = `CREATE INDEX ix_relation_tuple_by_subject ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)`
	createReverseCheckIndex = `CREATE INDEX ix_relation_tuple_by_subject_relation ON relation_tuple (userset_namespace, userset_relation, namespace, relation)`
)

func init() {
	if err := Manager.Register("add-reverse-index", "1eaeba4b8a73", func(apd *AlembicPostgresDriver) error {
		tx, err := apd.db.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, stmt := range []string{
			createReverseQueryIndex,
			createReverseCheckIndex,
		} {
			_, err := tx.Exec(stmt)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
