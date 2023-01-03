package migrations

import (
	"context"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
)

const (
	createCaveatTable = `CREATE TABLE caveat (
    	name STRING(MAX),
    	definition BYTES(MAX) NOT NULL,
    	timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
	) PRIMARY KEY (name)`

	addRelationshipCaveatName = `ALTER TABLE relation_tuple
		ADD COLUMN caveat_name STRING(MAX)`
	addRelationshipCaveatContext = `ALTER TABLE relation_tuple
		ADD COLUMN caveat_context JSON`

	addChangelogCaveatName = `ALTER TABLE changelog 
		ADD COLUMN caveat_name STRING(MAX)`
	addChangelogCaveatContext = `ALTER TABLE changelog
		ADD COLUMN caveat_context JSON`
)

func init() {
	if err := SpannerMigrations.Register("add-caveats", "add-metadata-and-counters", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				createCaveatTable,
				addRelationshipCaveatName,
				addRelationshipCaveatContext,
				addChangelogCaveatName,
				addChangelogCaveatContext,
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
