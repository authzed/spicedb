package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	// NOTE: We use 2 days here because Spanner only supports deletion policies at intervals of days.
	// See: https://cloud.google.com/spanner/docs/ttl/working-with-ttl#create
	addTransactionMetadataTable = `CREATE TABLE transaction_metadata (
			transaction_tag STRING(36) NOT NULL,
			created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
			metadata JSON
		) PRIMARY KEY (transaction_tag),
		ROW DELETION POLICY (OLDER_THAN(created_at, INTERVAL 2 DAY))
	`
)

func init() {
	if err := SpannerMigrations.Register("add-transaction-metadata-table", "add-relationship-counter-table", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				addTransactionMetadataTable,
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
