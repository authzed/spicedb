package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/migrate"
)

func init() {
	if err := CRDBMigrations.Register("refresh-schema-tables", "populate-schema-tables", noNonAtomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		writes := &pgx.Batch{}
		if !migrate.ShouldRefreshSchema(ctx) {
			writes.Queue(`INSERT INTO schema_revision (name, hash) VALUES ($1, $2)`, common.SchemaMigrationMarkerName, []byte(common.SchemaMigrationSourceUnified))
			return sendSchemaWrites(ctx, tx, writes)
		}
		if err := populateSchemaTables(ctx, tx, writes, true); err != nil {
			return err
		}
		// Exposing cluster_logical_timestamp pins this transaction's commit
		// timestamp, so the boundary agrees with MVCC visibility. It is read in
		// the same batch as the writes so no extra round trip holds their intents.
		writes.Queue(`INSERT INTO schema_revision (name, hash) VALUES ($1, ($2 || cluster_logical_timestamp()::STRING)::BYTES)`,
			common.SchemaMigrationMarkerName, common.SchemaMigrationSourceLegacy+":")
		return sendSchemaWrites(ctx, tx, writes)
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
