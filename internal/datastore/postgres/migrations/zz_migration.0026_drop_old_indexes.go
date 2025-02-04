package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const dropOldTransactionIndex = `
	DROP INDEX CONCURRENTLY IF EXISTS ix_relation_tuple_transaction_by_timestamp;
`

const dropOldRelationTupleIndex1 = `
	DROP INDEX CONCURRENTLY IF EXISTS ix_relation_tuple_by_subject;
`

const dropOldRelationTupleIndex2 = `
	DROP INDEX CONCURRENTLY IF EXISTS ix_relation_tuple_by_subject_relation;
`

const dropOldRelationTupleIndex3 = `
	DROP INDEX CONCURRENTLY IF EXISTS ix_relation_tuple_alive_by_resource_rel_subject_covering;
`

func init() {
	if err := DatabaseMigrations.Register("drop-old-indexes", "add-new-reverse-relationship-index",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, dropOldTransactionIndex); err != nil {
				return fmt.Errorf("failed to drop old relation tuple index 3: %w", err)
			}
			if _, err := conn.Exec(ctx, dropOldRelationTupleIndex1); err != nil {
				return fmt.Errorf("failed to drop old relation tuple index 1: %w", err)
			}
			if _, err := conn.Exec(ctx, dropOldRelationTupleIndex2); err != nil {
				return fmt.Errorf("failed to drop old relation tuple index 2: %w", err)
			}
			if _, err := conn.Exec(ctx, dropOldRelationTupleIndex3); err != nil {
				return fmt.Errorf("failed to drop old relation tuple index 3: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
