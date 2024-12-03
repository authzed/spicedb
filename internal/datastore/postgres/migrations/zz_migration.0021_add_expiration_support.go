package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const addExpirationColumn = `
	ALTER TABLE relation_tuple
	ADD COLUMN expiration TIMESTAMPTZ DEFAULT NULL;
`

const addWithExpirationCoveringIndex = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS ix_relation_tuple_with_expiration
	ON relation_tuple (namespace, relation, object_id, created_xid, expiration)
    INCLUDE (userset_namespace, userset_object_id, userset_relation, caveat_name, caveat_context)
    WHERE deleted_xid = '9223372036854775807'::xid8;`

const addExpiredRelationshipsIndex = `CREATE INDEX CONCURRENTLY
	IF NOT EXISTS ix_relation_tuple_expired
	ON relation_tuple (expiration)
	WHERE expiration IS NOT NULL;
`

func init() {
	if err := DatabaseMigrations.Register("add-expiration-support", "add-watch-api-index-to-relation-tuple-table",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, addExpirationColumn); err != nil {
				return fmt.Errorf("failed to add expiration column to relation tuple table: %w", err)
			}

			if _, err := conn.Exec(ctx, addWithExpirationCoveringIndex); err != nil {
				return fmt.Errorf("failed to add expiration column to relation tuple table: %w", err)
			}

			if _, err := conn.Exec(ctx, addExpiredRelationshipsIndex); err != nil {
				return fmt.Errorf("failed to add expiration column to relation tuple table: %w", err)
			}

			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
