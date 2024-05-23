package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	addRelationshipCountersTable = `CREATE TABLE relationship_counter (
			name STRING(MAX) NOT NULL,
			serialized_filter BYTES(MAX) NOT NULL,
			current_count INT64 NOT NULL,
			updated_at_timestamp TIMESTAMP,
		) PRIMARY KEY (name)
	`
)

func init() {
	if err := SpannerMigrations.Register("add-relationship-counter-table", "delete-older-changestreams", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				addRelationshipCountersTable,
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
