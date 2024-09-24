package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const createAliveRelByResourceRelationSubjectIndex = `CREATE INDEX CONCURRENTLY 
	IF NOT EXISTS ix_relation_tuple_alive_by_resource_rel_subject_covering
	ON relation_tuple (namespace, relation, userset_namespace)
    INCLUDE (userset_object_id, userset_relation, caveat_name, caveat_context)
    WHERE deleted_xid = '9223372036854775807'::xid8;`

func init() {
	if err := DatabaseMigrations.Register("add-rel-by-alive-resource-relation-subject", "add-tuned-gc-index",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, createAliveRelByResourceRelationSubjectIndex); err != nil {
				return fmt.Errorf("failed to create index for alive relationships by resource/relation/subject: %w", err)
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
