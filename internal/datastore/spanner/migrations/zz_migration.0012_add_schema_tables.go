package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	createSchemaTable = `CREATE TABLE schema (
		name STRING(1024) NOT NULL,
		chunk_index INT64 NOT NULL,
		chunk_data BYTES(MAX) NOT NULL,
		timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY (name, chunk_index)`

	createSchemaRevisionTable = `CREATE TABLE schema_revision (
		name STRING(1024) NOT NULL,
		schema_hash BYTES(MAX) NOT NULL,
		timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY (name)`
)

func init() {
	if err := SpannerMigrations.Register("add-schema-tables", "add-expiration-support", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createSchemaTable,
				createSchemaRevisionTable,
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
