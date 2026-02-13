package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	createSchemaTable = `CREATE TABLE schema (
		name VARCHAR NOT NULL,
		chunk_index INT NOT NULL,
		chunk_data BYTEA NOT NULL,
		timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
		CONSTRAINT pk_schema PRIMARY KEY (name, chunk_index)
	);`

	createSchemaRevisionTable = `CREATE TABLE schema_revision (
		name VARCHAR NOT NULL DEFAULT 'current',
		hash BYTEA NOT NULL,
		timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
		CONSTRAINT pk_schema_revision PRIMARY KEY (name)
	);`
)

func init() {
	err := CRDBMigrations.Register("add-schema-tables", "add-expiration-support", addSchemaTablesFunc, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addSchemaTablesFunc(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, createSchemaTable); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, createSchemaRevisionTable); err != nil {
		return err
	}
	return nil
}
