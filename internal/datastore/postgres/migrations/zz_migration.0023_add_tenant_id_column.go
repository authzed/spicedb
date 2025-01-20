package migrations

import (
	"context"
	"github.com/jackc/pgx/v5"
)

const addTenantIDColumnToRelationTupleTable = `ALTER TABLE relation_tuple ADD COLUMN IF NOT EXISTS tenant_id VARCHAR NOT NULL DEFAULT '';`
const createIndexForRelationTupleTenantID = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS idx_relation_tuple_tenant_id 
	ON relation_tuple (tenant_id);`

func init() {
	err := DatabaseMigrations.Register(
		"add-tenant-id-column-to-relation-tuple-table",
		"add-index-for-transaction-gc",
		noNonatomicMigration,
		func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, addTenantIDColumnToRelationTupleTable); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, createIndexForRelationTupleTenantID); err != nil {
				return err
			}
			return nil
		},
	)
	if err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
