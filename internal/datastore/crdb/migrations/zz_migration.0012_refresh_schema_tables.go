package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/migrate"
)

func init() {
	if err := CRDBMigrations.Register("refresh-schema-tables", "populate-schema-tables", noNonAtomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		source := common.SchemaMigrationSourceUnified
		if migrate.ShouldRefreshSchema(ctx) {
			if err := populateSchemaTables(ctx, tx, true); err != nil {
				return err
			}
			// Exposing cluster_logical_timestamp pins this transaction's commit
			// timestamp, so the boundary agrees with MVCC visibility.
			var revision string
			if err := tx.QueryRow(ctx, `SELECT cluster_logical_timestamp()::STRING`).Scan(&revision); err != nil {
				return err
			}
			source = common.SchemaMigrationSourceLegacy + ":" + revision
		}
		_, err := tx.Exec(ctx, `INSERT INTO schema_revision (name, hash) VALUES ($1, $2)`, common.SchemaMigrationMarkerName, []byte(source))
		return err
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
