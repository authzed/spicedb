package migrations

import (
	"context"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/google/uuid"
)

const (
	createMetadata = `CREATE TABLE metadata (
		unique_id STRING(36),
	) PRIMARY KEY (unique_id)`

	createCounters = `CREATE TABLE relationship_estimate_counters (
		id BYTES(2) NOT NULL,
		count INT64 NOT NULL
	) PRIMARY KEY (id)`
)

func init() {
	if err := SpannerMigrations.Register("add-metadata-and-counters", "initial", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createMetadata,
				createCounters,
			},
		})
		if err != nil {
			return err
		}

		return updateOp.Wait(ctx)
	}, func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
		return rwt.BufferWrite([]*spanner.Mutation{
			spanner.Insert("metadata", []string{"unique_id"}, []interface{}{uuid.NewString()}),
		})
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
