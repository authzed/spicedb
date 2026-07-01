package crdb

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/pkg/tuple"
)

// createChangelogTableSQL returns the DDL for the experimental changelog table.
// The table is intentionally created outside the linear migrate chain so it is
// only present when the experimental flag is set (prototype decision).
//
// A single table with a `kind` discriminator holds both relationship and schema
// changes. Each row is self-contained so Watch never joins back to the
// relationship tables. The primary key is hash-sharded over the monotonic
// change_ts to avoid a write hotspot. Row-level TTL matches the GC window;
// ttl_disable_changefeed_replication keeps TTL deletes from generating nudge
// noise.
//
// gcWindow is accepted but not yet threaded into the DDL: TTL is expressed
// via ttl_expiration_expression against the stored ttl_expiration column
// rather than a fixed interval. The parameter is kept so callers won't need
// to change once the GC window is wired into the expiration calculation.
//
//nolint:unparam // see above; parameter reserved for future use
func createChangelogTableSQL(gcWindow time.Duration) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	%s DECIMAL NOT NULL,
	%s INT NOT NULL,
	%s STRING NOT NULL,
	%s STRING, %s STRING, %s STRING,
	%s STRING, %s STRING, %s STRING,
	%s STRING, %s JSONB, %s TIMESTAMPTZ,
	%s STRING,
	%s STRING, %s STRING, %s BYTES,
	%s TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (%s, %s) USING HASH
) WITH (
	ttl_expiration_expression = '%s',
	ttl_job_cron = '@hourly',
	ttl_disable_changefeed_replication = 'true'
)`,
		schema.TableRelationshipChangelog,
		schema.ColChangeTS,
		schema.ColChangeOrdinal,
		schema.ColChangeKind,
		schema.ColNamespace, schema.ColObjectID, schema.ColRelation,
		schema.ColUsersetNamespace, schema.ColUsersetObjectID, schema.ColUsersetRelation,
		schema.ColCaveatContextName, schema.ColCaveatContext, schema.ColChangeRelExpiration,
		schema.ColChangeOperation,
		schema.ColChangeSchemaKind, schema.ColChangeDefinitionName, schema.ColChangeSerializedDefinition,
		schema.ColChangeTTLExpiration,
		schema.ColChangeTS, schema.ColChangeOrdinal,
		schema.ColChangeTTLExpiration,
	)
}

// ensureChangelogTable creates the changelog table if it does not already exist.
func ensureChangelogTable(ctx context.Context, initPool *pool.RetryPool, gcWindow time.Duration) error {
	return initPool.ExecFunc(ctx, func(ctx context.Context, tag pgconn.CommandTag, err error) error {
		return err
	}, createChangelogTableSQL(gcWindow))
}

// operationToChangelog maps a tuple update operation to its stored string.
func operationToChangelog(op tuple.UpdateOperation) (string, error) {
	switch op {
	case tuple.UpdateOperationCreate:
		return "create", nil
	case tuple.UpdateOperationTouch:
		return "touch", nil
	case tuple.UpdateOperationDelete:
		return "delete", nil
	default:
		return "", fmt.Errorf("unknown changelog operation: %v", op)
	}
}

// appendRelationshipChangelog inserts a single relationship change row into the
// changelog in the current transaction. change_ts is set to the transaction's
// HLC via cluster_logical_timestamp() so it matches the relationship write's
// commit revision exactly.
func (rwt *crdbReadWriteTXN) appendRelationshipChangelog(ctx context.Context, rel tuple.Relationship, op tuple.UpdateOperation, ordinal int, ttlExpiration time.Time) error {
	opString, err := operationToChangelog(op)
	if err != nil {
		return err
	}

	var caveatName string
	var caveatContext map[string]any
	if rel.OptionalCaveat != nil {
		caveatName = rel.OptionalCaveat.CaveatName
		caveatContext = rel.OptionalCaveat.Context.AsMap()
	}

	insert := psql.Insert(schema.TableRelationshipChangelog).Columns(
		schema.ColChangeTS,
		schema.ColChangeOrdinal,
		schema.ColChangeKind,
		schema.ColNamespace, schema.ColObjectID, schema.ColRelation,
		schema.ColUsersetNamespace, schema.ColUsersetObjectID, schema.ColUsersetRelation,
		schema.ColCaveatContextName, schema.ColCaveatContext, schema.ColChangeRelExpiration,
		schema.ColChangeOperation,
		schema.ColChangeTTLExpiration,
	).Values(
		sq.Expr("cluster_logical_timestamp()"),
		ordinal,
		"rel",
		rel.Resource.ObjectType, rel.Resource.ObjectID, rel.Resource.Relation,
		rel.Subject.ObjectType, rel.Subject.ObjectID, rel.Subject.Relation,
		caveatName, caveatContext, rel.OptionalExpiration,
		opString,
		ttlExpiration,
	)

	sql, args, err := insert.ToSql()
	if err != nil {
		return fmt.Errorf("unable to build changelog insert: %w", err)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("unable to write changelog row: %w", err)
	}
	return nil
}
