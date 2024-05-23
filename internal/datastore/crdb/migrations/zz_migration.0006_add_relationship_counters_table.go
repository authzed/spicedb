package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	addRelationshipCountersTableQuery = `
		CREATE TABLE relationship_counter (
			name TEXT PRIMARY KEY,
			serialized_filter BYTEA NOT NULL,
			current_count BIGINT NOT NULL DEFAULT 0,
			updated_at_timestamp DECIMAL DEFAULT NULL
		);
	`
)

func init() {
	err := CRDBMigrations.Register("add-relationship-counters-table", "remove-stats-table", addRelationshipCountersTable, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addRelationshipCountersTable(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, addRelationshipCountersTableQuery); err != nil {
		return err
	}
	return nil
}
