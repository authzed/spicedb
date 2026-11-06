package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/migrate"
)

func init() {
	if err := DatabaseMigrations.Register("refresh-schema-tables", "populate-schema-tables", noNonatomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		source := common.SchemaMigrationSourceUnified
		if migrate.ShouldRefreshSchema(ctx) {
			source = common.SchemaMigrationSourceLegacy

			// Participate in SSI with preparation-phase writers. A writer whose snapshot
			// predates the first unified row must retry, not insert a second live version.
			if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`); err != nil {
				return err
			}
			// Wait for schema updates already in flight before copying legacy storage.
			if _, err := tx.Exec(ctx, `LOCK TABLE namespace_config, caveat, schema, schema_revision IN SHARE ROW EXCLUSIVE MODE`); err != nil {
				return err
			}

			if err := populateSchemaTables(ctx, tx, true); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO schema_revision (name, hash) VALUES ($1, $2)`, common.SchemaMigrationMarkerName, []byte(source))
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
