package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	addRelationshipIndexStoringCaveatQuery = `
		CREATE INDEX idx_relationship_caveat_storing ON relation_tuple (namespace, relation, object_id, userset_relation) STORING (caveat_name, caveat_context);
	`
)

func init() {
	err := CRDBMigrations.Register("add-relationship-index-storing-caveat", "add-relationship-counters-table", addRelationshipIndexStoringCaveat, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addRelationshipIndexStoringCaveat(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, addRelationshipIndexStoringCaveatQuery); err != nil {
		return err
	}
	return nil
}
