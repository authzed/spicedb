package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	createCaveatsCoveringIndex = `CREATE INDEX ix_relation_tuple_caveat_covering ON relation_tuple
	(namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, caveat_name) 
	STORING 
	(caveat_context)`
)

func init() {
	err := CRDBMigrations.Register("add-caveat-covering-index", "add-caveats", addCaveatToTuplesIndexFunc, noAtomicMigration)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

func addCaveatToTuplesIndexFunc(ctx context.Context, conn *pgx.Conn) error {
	if _, err := conn.Exec(ctx, createCaveatsCoveringIndex); err != nil {
		return err
	}
	// A primary key's index cannot be dropped
	return nil
}
