package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/migrate"
)

var (
	addRelationTupleDefault = `
		ALTER TABLE relation_tuple
			ALTER COLUMN created_xid SET DEFAULT (pg_current_xact_id());`

	addNamepsaceDefault = `
		ALTER TABLE namespace_config
			ALTER COLUMN created_xid SET DEFAULT (pg_current_xact_id());`

	addCaveatDefault = `
		ALTER TABLE caveat
			ALTER COLUMN created_xid SET DEFAULT (pg_current_xact_id());`
)

var addBackfillIndices = []string{
	addRelationTupleDefault,
	addNamepsaceDefault,
	addCaveatDefault,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_backfill_rtt_temp
		ON relation_tuple_transaction ( (snapshot IS NULL) )`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_backfill_ns_temp
		ON namespace_config ( (created_xid IS NULL) )`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_backfill_tuple_temp
		ON relation_tuple ( (created_xid IS NULL) )`,
	`CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_backfill_caveat_temp
		ON caveat ( (created_xid IS NULL) )`,
}

var backfills = []string{
	`UPDATE relation_tuple_transaction 
		SET xid = id::text::xid8, snapshot = CONCAT(id, ':', id, ':')::pg_snapshot
		WHERE id IN (
			SELECT id FROM relation_tuple_transaction
			WHERE snapshot IS NULL
			LIMIT %d
			FOR UPDATE
		);`,
	`UPDATE relation_tuple 
		SET deleted_xid = deleted_transaction::text::xid8,
		created_xid = created_transaction::text::xid8
		WHERE (namespace, object_id, relation, userset_namespace, userset_object_id,
			   userset_relation, created_transaction, deleted_transaction
		) IN (
			SELECT namespace, object_id, relation, userset_namespace, userset_object_id,
				userset_relation, created_transaction, deleted_transaction
			FROM relation_tuple
			WHERE created_xid IS NULL
			LIMIT %d
			FOR UPDATE
		);`,
	`UPDATE namespace_config 
		SET deleted_xid = deleted_transaction::text::xid8,
		created_xid = created_transaction::text::xid8
		WHERE (namespace, created_transaction, deleted_transaction) IN (
			SELECT namespace, created_transaction, deleted_transaction
			FROM namespace_config
			WHERE created_xid IS NULL
			LIMIT %d
			FOR UPDATE
		);`,
	`UPDATE caveat 
		SET deleted_xid = deleted_transaction::text::xid8,
		created_xid = created_transaction::text::xid8
		WHERE (name, created_transaction, deleted_transaction) IN (
			SELECT name, created_transaction, deleted_transaction
			FROM caveat
			WHERE created_xid IS NULL
			LIMIT %d
			FOR UPDATE
		);`,
}

var addXIDIndices = []string{
	// Replace the indices that are inherent from having a primary key constraint
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_rttx_oldpk
		ON relation_tuple_transaction (id)`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_namespace_config_oldpk
		ON namespace_config (id)`,

	// Add indices that will eventually back our new constraints
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_rttx_pk
		ON relation_tuple_transaction (xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_namespace_config_pk
		ON namespace_config (namespace, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_namespace_config_living
		ON namespace_config (namespace, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_relation_tuple_pk
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
						   userset_relation, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_relation_tuple_living
		ON relation_tuple (namespace, object_id, relation, userset_namespace, userset_object_id,
						   userset_relation, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_caveat_unique
		ON caveat (name, created_xid, deleted_xid);`,
	`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ix_caveat_living
		ON caveat (name, deleted_xid);`,
}

var dropBackfillIndices = []string{
	"DROP INDEX ix_backfill_rtt_temp",
	"DROP INDEX ix_backfill_ns_temp",
	"DROP INDEX ix_backfill_tuple_temp",
	"DROP INDEX ix_backfill_caveat_temp",
}

func init() {
	if err := DatabaseMigrations.Register("backfill-xid-add-indices", "add-xid-columns",
		func(ctx context.Context, conn *pgx.Conn) error {
			for _, stmt := range addBackfillIndices {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					return err
				}
			}

			batchSize := ctx.Value(migrate.BackfillBatchSize).(uint64)
			for _, stmt := range backfills {
				concreteStmt := fmt.Sprintf(stmt, batchSize)

				log.Ctx(ctx).Info().Str("statement", concreteStmt).Msg("starting backfill")

				var r pgconn.CommandTag
				var err error

				for r, err = conn.Exec(ctx, concreteStmt); err == nil && r.RowsAffected() > 0; r, err = conn.Exec(ctx, concreteStmt) {
					log.Ctx(ctx).Debug().Int64("count", r.RowsAffected()).Msg("updated rows")
				}
				if err != nil {
					return err
				}
			}

			for _, stmt := range append(addXIDIndices, dropBackfillIndices...) {
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
