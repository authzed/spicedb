package migrations

const (
	createDeletedTransactionIndex = `CREATE INDEX ix_relation_tuple_by_deleted_transaction ON relation_tuple (deleted_transaction)`
)

func init() {
	if err := DatabaseMigrations.Register("add-gc-index", "change-transaction-timestamp-default", func(apd *AlembicPostgresDriver) error {
		tx, err := apd.db.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, stmt := range []string{
			createDeletedTransactionIndex,
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
