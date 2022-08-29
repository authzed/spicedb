package migrations

import (
	"context"

	"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	addCaveatToRelationTuple     = `ALTER TABLE relation_tuple ADD COLUMN caveat BYTES(4096)`
	addCaveatToRelationChangelog = `ALTER TABLE changelog ADD COLUMN caveat BYTES(4096)`
)

func init() {
	if err := SpannerMigrations.Register("add-caveat", "add-metadata-and-counters", func(ctx context.Context, w Wrapper) error {
		updateOp, err := w.adminClient.UpdateDatabaseDdl(ctx, &database.UpdateDatabaseDdlRequest{
			Database: w.client.DatabaseName(),
			Statements: []string{
				addCaveatToRelationTuple,
				addCaveatToRelationChangelog,
			},
		})
		if err != nil {
			return err
		}

		return updateOp.Wait(ctx)
	}, noReadWriteTransaction); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
