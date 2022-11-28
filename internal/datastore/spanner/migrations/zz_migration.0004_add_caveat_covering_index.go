package migrations

import (
	"context"

	"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// See https://cloud.google.com/spanner/docs/secondary-indexes
const createCaveatsCoveringIndex = `CREATE INDEX ix_relation_tuple_caveat_covering ON relation_tuple(namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, caveat_name) STORING (caveat_context)`

func init() {
	if err := SpannerMigrations.Register("add-caveat-covering-index", "add-caveats", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &database.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createCaveatsCoveringIndex,
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
