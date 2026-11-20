package migrations

import (
	"context"

	"cloud.google.com/go/spanner"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/migrate"
)

func init() {
	if err := SpannerMigrations.Register("refresh-schema-tables", "populate-schema-tables", nil, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		source := common.SchemaMigrationSourceUnified
		if migrate.ShouldRefreshSchema(ctx) {
			if err := populateSchemaTables(ctx, tx, true); err != nil {
				return err
			}
			source = common.SchemaMigrationSourceLegacy
		}
		return tx.BufferWrite([]*spanner.Mutation{spanner.Insert("schema_revision",
			[]string{"name", "schema_hash", "timestamp"},
			[]any{common.SchemaMigrationMarkerName, []byte(source), spanner.CommitTimestamp})})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
