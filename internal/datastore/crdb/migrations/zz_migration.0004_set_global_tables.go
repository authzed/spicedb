package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

const (
	makeNamespaceGlobal = `ALTER TABLE namespace_config SET LOCALITY GLOBAL`
	makeTuplesGlobal    = `ALTER TABLE relation_tuple SET LOCALITY GLOBAL`
	makeCountersGlobal  = `ALTER TABLE relationship_estimate_counters SET LOCALITY GLOBAL`
)

func init() {
	if err := CRDBMigrations.Register("set-global-tables", "add-metadata-and-counters", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, makeNamespaceGlobal); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, makeTuplesGlobal); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, makeCountersGlobal); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
