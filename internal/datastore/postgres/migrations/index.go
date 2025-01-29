package migrations

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/postgres/schema"
)

func registerIndexMigration(index common.IndexDefinition, currentStep string, previousStep string) {
	if err := DatabaseMigrations.Register(currentStep, previousStep,
		func(ctx context.Context, conn *pgx.Conn) error {
			return schema.CreateIndexConcurrently(ctx, conn, index)
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
