package migrations

import (
	"context"

	"github.com/jackc/pgx/v4"
)

// See https://www.postgresql.org/docs/current/indexes-index-only-scans.html
// New index introduced to support relationship queries that now include caveat columns.
// JSON columns cannot be part of index, but can be included in the index for retrieval using "INCLUDE"
const createCaveatsCoveringIndex = `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_relation_tuple_living_covering
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, caveat_name, deleted_xid) 
        INCLUDE (created_xid, caveat_context);`

// also used by relationship query patterns to handle Postgres Datastore explicit MVCC logic
const createTransactionCoveringIndex = `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_rttx_pk_covering
		ON relation_tuple_transaction (xid) INCLUDE (snapshot);`

func init() {
	if err := DatabaseMigrations.Register("add-caveat-covering-index", "drop-bigserial-ids",
		func(ctx context.Context, conn *pgx.Conn) error {
			if _, err := conn.Exec(ctx, createCaveatsCoveringIndex); err != nil {
				return err
			}
			if _, err := conn.Exec(ctx, createTransactionCoveringIndex); err != nil {
				return err
			}
			return nil
		},
		noTxMigration); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
