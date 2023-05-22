package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

var createRelationTupleChangeStream = `CREATE CHANGE STREAM relation_tuple_stream FOR relation_tuple`

func init() {
	if err := SpannerMigrations.Register("register-tuple-change-stream", "add-caveats", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createRelationTupleChangeStream,
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
