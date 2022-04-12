package migrations

import (
	"context"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
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
	if err := SpannerMigrations.Register("add-metadata-and-counters", "initial", func(smd SpannerMigrationDriver) error {
		ctx := context.Background()

		updateOp, err := smd.adminClient.UpdateDatabaseDdl(ctx, &database.UpdateDatabaseDdlRequest{
			Database: smd.client.DatabaseName(),
			Statements: []string{
				createMetadata,
				createCounters,
			},
		})
		if err != nil {
			return err
		}

		if err := updateOp.Wait(ctx); err != nil {
			return err
		}

		if _, err := smd.client.Apply(ctx, []*spanner.Mutation{
			spanner.Insert("metadata", []string{"unique_id"}, []interface{}{uuid.NewString()}),
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
