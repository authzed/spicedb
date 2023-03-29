package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"
)

const (
	dropNSConfigPK   = `ALTER TABLE namespace_config DROP CONSTRAINT IF EXISTS pk_namespace_config`
	createNSConfigID = `ALTER TABLE namespace_config ADD COLUMN id BIGSERIAL PRIMARY KEY`
)

func init() {
	if err := DatabaseMigrations.Register(
		"add-ns-config-id",
		"add-unique-datastore-id",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, dropNSConfigPK); err != nil {
				return err
			}

			if _, err := tx.Exec(ctx, createNSConfigID); err != nil {
				return err
			}

			return nil
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
