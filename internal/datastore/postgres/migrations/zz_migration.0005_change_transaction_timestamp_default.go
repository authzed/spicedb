package migrations

import (
	"context"
)

const alterTimestampDefaultValue = `
	ALTER TABLE relation_tuple_transaction 
		ALTER COLUMN timestamp SET DEFAULT (now() AT TIME ZONE 'UTC');`

func init() {
	if err := DatabaseMigrations.Register("change-transaction-timestamp-default", "add-transaction-timestamp-index", func(apd *AlembicPostgresDriver) error {
		_, err := apd.db.Exec(context.Background(), alterTimestampDefaultValue)
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
