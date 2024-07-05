package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const (
	createRelationTupleWithIntegrityTable = `CREATE TABLE relation_tuple_with_integrity (
    namespace VARCHAR NOT NULL,
    object_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    userset_namespace VARCHAR NOT NULL,
    userset_object_id VARCHAR NOT NULL,
    userset_relation VARCHAR NOT NULL,
	caveat_name VARCHAR,
	caveat_context JSONB,
	timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
	integrity_hash BYTEA NOT NULL,
	integrity_key_id VARCHAR(255) NOT NULL,
    CONSTRAINT pk_relation_tuple PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)
);`

	createIntegrityTupleIndex = `CREATE INDEX ix_relation_tuple_with_integrity ON relation_tuple_with_integrity (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation) STORING (integrity_key_id, integrity_hash, timestamp, caveat_name, caveat_context);`
)

func init() {
	err := CRDBMigrations.Register("add-integrity-relationtuple-table", "add-relationship-counters-table", addIntegrityColumns, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addIntegrityColumns(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, createRelationTupleWithIntegrityTable); err != nil {
		return fmt.Errorf("failed to create relation_tuple_with_integrity table: %w", err)
	}

	if _, err := conn.Exec(ctx, createIntegrityTupleIndex); err != nil {
		return fmt.Errorf("failed to create index on relation_tuple_with_integrity table: %w", err)
	}

	return nil
}
