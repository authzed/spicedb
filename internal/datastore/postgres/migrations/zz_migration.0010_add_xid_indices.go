package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

var addXIDIndices = []string{
	// Replace the indices that are inherent from having a primary key constraint
	"CREATE UNIQUE INDEX CONCURRENTLY ix_rttx_oldpk ON relation_tuple_transaction (id)",
	"CREATE UNIQUE INDEX CONCURRENTLY ix_namespace_config_oldpk ON namespace_config (id)",

	// Add indices that will eventually back our new constraints
	"CREATE UNIQUE INDEX CONCURRENTLY ix_rttx_pk ON relation_tuple_transaction (xid);",
	`CREATE UNIQUE INDEX CONCURRENTLY ix_namespace_config_pk
		ON namespace_config (namespace, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY ix_namespace_config_living
		ON namespace_config (namespace, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY ix_relation_tuple_pk
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
						   userset_relation, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY ix_relation_tuple_living
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
						   userset_relation, deleted_xid);`,
}

func init() {
	if err := DatabaseMigrations.Register("add-xid-indices", "add-xid-columns",
		func(ctx context.Context, conn *pgx.Conn) error {
			for _, stmt := range addXIDIndices {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			return nil
		},
		noTxMigration,
	); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
