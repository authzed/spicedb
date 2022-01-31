package migrations

const createIndexOnTupleTransactionTimestamp = `
	CREATE INDEX ix_relation_tuple_transaction_by_timestamp on relation_tuple_transaction(timestamp);
`

func init() {
	if err := Manager.Register("add-transaction-timestamp-index", "add-unique-living-ns", func(apd *AlembicPostgresDriver) error {
		tx, err := apd.db.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		_, err = tx.Exec(createIndexOnTupleTransactionTimestamp)
		if err != nil {
			return err
		}

		return tx.Commit()
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
