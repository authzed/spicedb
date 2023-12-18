package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

var createCombinedChangeStream = `CREATE CHANGE STREAM combined_change_stream FOR namespace_config,caveat,relation_tuple`

func init() {
	if err := SpannerMigrations.Register("register-combined-change-stream", "register-schema-change-stream", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createCombinedChangeStream,
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
