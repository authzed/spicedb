package migrations

const alterTimestampDefaultValue = `
	ALTER TABLE relation_tuple_transaction 
		ALTER COLUMN timestamp SET DEFAULT (now() AT TIME ZONE 'UTC');`

func init() {
	if err := DatabaseMigrations.Register("change-transaction-timestamp-default", "add-transaction-timestamp-index", func(apd *AlembicPostgresDriver) error {
		tx, err := apd.db.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.Exec(alterTimestampDefaultValue)
		if err != nil {
			return err
		}

		return tx.Commit()
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
