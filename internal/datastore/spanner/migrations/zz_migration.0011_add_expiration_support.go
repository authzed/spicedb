package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	addExpirationColumn = `
		ALTER TABLE relation_tuple ADD COLUMN expires_at TIMESTAMP
	`

	addExpirationSupport = `
		ALTER TABLE relation_tuple ADD ROW DELETION POLICY (OLDER_THAN(expires_at, INTERVAL 1 DAY))
	`
)

func init() {
	if err := SpannerMigrations.Register("add-expiration-support", "add-transaction-metadata-table", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				addExpirationColumn,
				addExpirationSupport,
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
