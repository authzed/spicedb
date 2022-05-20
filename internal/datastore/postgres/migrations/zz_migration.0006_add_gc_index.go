package migrations

import "context"

const (
	createDeletedTransactionIndex = `CREATE INDEX CONCURRENTLY ix_relation_tuple_by_deleted_transaction ON relation_tuple (deleted_transaction)`
)

func init() {
	if err := DatabaseMigrations.Register("add-gc-index", "change-transaction-timestamp-default", func(apd *AlembicPostgresDriver) error {
		_, err := apd.db.Exec(context.Background(), createDeletedTransactionIndex)
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
