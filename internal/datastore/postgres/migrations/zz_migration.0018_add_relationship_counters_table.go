package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const createRelationshipCountersTable = `CREATE TABLE relationship_counter (
    name VARCHAR NOT NULL,
	serialized_filter BYTEA NOT NULL,
	current_count BIGINT NOT NULL DEFAULT 0,
	updated_revision_snapshot pg_snapshot,
    created_xid xid8 NOT NULL DEFAULT (pg_current_xact_id()),
	deleted_xid xid8 NOT NULL DEFAULT ('9223372036854775807'),
    CONSTRAINT pk_relationship_counter PRIMARY KEY (name),
    CONSTRAINT uq_relationship_counter_living UNIQUE (name, deleted_xid)
);`

func init() {
	if err := DatabaseMigrations.Register("create-relationships-counters-table", "add-rel-by-alive-resource-relation-subject",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, createRelationshipCountersTable); err != nil {
				return fmt.Errorf("failed to create relationships counters table: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
