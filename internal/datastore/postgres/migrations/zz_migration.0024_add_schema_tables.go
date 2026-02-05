package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

var schemaTablesStatements = []string{
	`CREATE TABLE schema (
		name VARCHAR NOT NULL,
		chunk_index INT NOT NULL,
		chunk_data BYTEA NOT NULL,
		created_xid xid8 NOT NULL DEFAULT (pg_current_xact_id()),
		deleted_xid xid8 NOT NULL DEFAULT ('9223372036854775807'),
		CONSTRAINT pk_schema PRIMARY KEY (name, chunk_index, created_xid));`,
	`CREATE INDEX ix_schema_gc ON schema (deleted_xid DESC) WHERE deleted_xid < '9223372036854775807'::xid8;`,
	`CREATE TABLE schema_revision (
		name VARCHAR NOT NULL DEFAULT 'current',
		hash BYTEA NOT NULL,
		created_xid xid8 NOT NULL DEFAULT (pg_current_xact_id()),
		deleted_xid xid8 NOT NULL DEFAULT ('9223372036854775807'),
		CONSTRAINT pk_schema_revision PRIMARY KEY (name, created_xid));`,
	`CREATE INDEX ix_schema_revision_gc ON schema_revision (deleted_xid DESC) WHERE deleted_xid < '9223372036854775807'::xid8;`,
}

func init() {
	if err := DatabaseMigrations.Register("add-schema-tables", "add-index-for-transaction-gc",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range schemaTablesStatements {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
