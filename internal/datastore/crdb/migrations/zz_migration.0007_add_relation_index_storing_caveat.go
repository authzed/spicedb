package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	addRelationshipIndexStoringCaveatQuery = `
	-- Drop existing indexes
	DROP INDEX IF EXISTS ix_relation_tuple_by_subject;
	DROP INDEX IF EXISTS ix_relation_tuple_by_subject_relation;

	-- Create new indexes with the STORING clause
	CREATE INDEX ix_relation_tuple_by_subject_storing_caveat
	ON relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)
	STORING (caveat_name, caveat_context);

	CREATE INDEX ix_relation_tuple_by_subject_relation_storing_caveat
	ON relation_tuple (userset_namespace, userset_relation, namespace, relation)
	STORING (caveat_name, caveat_context);
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
