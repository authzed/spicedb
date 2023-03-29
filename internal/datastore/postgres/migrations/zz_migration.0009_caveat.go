package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

var caveatStatements = []string{
	`CREATE TABLE caveat (
		name VARCHAR NOT NULL,
		definition BYTEA NOT NULL,
		created_transaction BIGINT NOT NULL,
		deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
		CONSTRAINT pk_caveat_v1 PRIMARY KEY (name, deleted_transaction),
		CONSTRAINT uq_caveat_v1 UNIQUE (name, created_transaction, deleted_transaction));`,
	`ALTER TABLE relation_tuple
		ADD COLUMN caveat_name VARCHAR,
		ADD COLUMN caveat_context JSONB;`,
}

func init() {
	if err := DatabaseMigrations.Register("add-caveats", "add-ns-config-id",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			for _, stmt := range caveatStatements {
				if _, err := tx.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
