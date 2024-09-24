package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

var createSchemaChangeStream = `CREATE CHANGE STREAM schema_change_stream FOR namespace_config,caveat`

func init() {
	if err := SpannerMigrations.Register("register-schema-change-stream", "drop-changelog-table", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createSchemaChangeStream,
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
