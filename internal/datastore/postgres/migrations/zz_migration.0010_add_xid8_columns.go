package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	addTransactionXIDColumns = `
		ALTER TABLE relation_tuple_transaction
			ALTER COLUMN id SET DEFAULT (pg_current_xact_id()::text::bigint),
			ADD COLUMN xid xid8 NOT NULL DEFAULT (pg_current_xact_id()),
			ADD COLUMN snapshot pg_snapshot`

	addTupleXIDColumns = `
		ALTER TABLE relation_tuple
			ADD COLUMN created_xid xid8,
			ADD COLUMN deleted_xid xid8 NOT NULL DEFAULT ('9223372036854775807');`

	addNamespaceXIDColumns = `
		ALTER TABLE namespace_config
			ADD COLUMN created_xid xid8,
			ADD COLUMN deleted_xid xid8 NOT NULL DEFAULT ('9223372036854775807');`

	addCaveatXIDColumns = `
		ALTER TABLE caveat
			ADD COLUMN created_xid xid8,
			ADD COLUMN deleted_xid xid8 NOT NULL DEFAULT ('9223372036854775807');`

	addTransactionDefault = `
		ALTER TABLE relation_tuple_transaction
			ALTER COLUMN snapshot SET DEFAULT (pg_current_snapshot());`

	addRelationTupleDefault = `
		ALTER TABLE relation_tuple
			ALTER COLUMN created_xid SET DEFAULT (pg_current_xact_id());`

	addNamepsaceDefault = `
		ALTER TABLE namespace_config
			ALTER COLUMN created_xid SET DEFAULT (pg_current_xact_id());`

	addCaveatDefault = `
		ALTER TABLE caveat
			ALTER COLUMN created_xid SET DEFAULT (pg_current_xact_id());`
)

func init() {
	if err := DatabaseMigrations.Register("add-xid-columns", "add-caveats",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range []string{
				addTransactionXIDColumns,
				addTupleXIDColumns,
				addNamespaceXIDColumns,
				addCaveatXIDColumns,
				addTransactionDefault,
				addRelationTupleDefault,
				addNamepsaceDefault,
				addCaveatDefault,
			} {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
