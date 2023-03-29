package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
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
	err := CRDBMigrations.Register("add-caveats", "add-metadata-and-counters", addCaveatFunc, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addCaveatFunc(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, createCaveatTable); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, addRelationshipCaveatContext); err != nil {
		return err
	}
	return nil
}
