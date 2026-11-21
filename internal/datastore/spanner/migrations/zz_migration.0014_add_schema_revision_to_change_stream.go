package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

// schema_revision is written by every write of the unified stored schema, so watching it
// allows the Watch API to observe schema changes made to the unified stored schema.
var addSchemaRevisionToChangeStream = `ALTER CHANGE STREAM combined_change_stream SET FOR namespace_config,caveat,relation_tuple,schema_revision`

func init() {
	if err := SpannerMigrations.Register("add-schema-revision-to-change-stream", "populate-schema-tables", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				addSchemaRevisionToChangeStream,
			},
		})
		if err != nil {
			return err
		}
		return updateOp.Wait(ctx)
	}, nil); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
