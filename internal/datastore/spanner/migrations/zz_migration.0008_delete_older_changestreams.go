package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	deleteTupleChangeStream  = `DROP CHANGE STREAM relation_tuple_stream`
	deleteSchemaChangeStream = `DROP CHANGE STREAM schema_change_stream`
)

func init() {
	if err := SpannerMigrations.Register("delete-older-changestreams", "register-combined-change-stream", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				deleteTupleChangeStream,
				deleteSchemaChangeStream,
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
