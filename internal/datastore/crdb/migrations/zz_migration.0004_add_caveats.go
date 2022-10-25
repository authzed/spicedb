package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	createCaveatTable = `CREATE TABLE caveat (
		name VARCHAR NOT NULL,
		definition BYTEA NOT NULL,
		timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
		CONSTRAINT pk_caveat_v1 PRIMARY KEY (name)
	);`

	addRelationshipCaveatContext = `ALTER TABLE relation_tuple
		ADD COLUMN caveat_name VARCHAR,
		ADD COLUMN caveat_context JSONB;`
)

func init() {
	if err := CRDBMigrations.Register("add-caveats", "add-metadata-and-counters", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, createCaveatTable); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, addRelationshipCaveatContext); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
