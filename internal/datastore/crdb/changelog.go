package crdb

import (
	"context"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
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

// appendSchemaChangelog inserts a single schema change row. A nil serialized
// payload records a deletion of definitionName.
func (rwt *crdbReadWriteTXN) appendSchemaChangelog(ctx context.Context, schemaKind, definitionName string, serialized []byte, ordinal int, ttlExpiration time.Time) error {
	insert := psql.Insert(schema.TableRelationshipChangelog).Columns(
		schema.ColChangeTS,
		schema.ColChangeOrdinal,
		schema.ColChangeKind,
		schema.ColChangeSchemaKind, schema.ColChangeDefinitionName, schema.ColChangeSerializedDefinition,
		schema.ColChangeTTLExpiration,
	).Values(
		sq.Expr("cluster_logical_timestamp()"),
		ordinal,
		"schema",
		schemaKind, definitionName, serialized,
		ttlExpiration,
	)

	sql, args, err := insert.ToSql()
	if err != nil {
		return fmt.Errorf("unable to build schema changelog insert: %w", err)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("unable to write schema changelog row: %w", err)
	}
	return nil
}

// changelogTTL returns the TTL expiration to stamp on changelog rows.
func (rwt *crdbReadWriteTXN) changelogTTL() time.Time {
	return time.Now().Add(rwt.gcWindow).Add(1 * time.Minute)
}

// capturingBulkSource wraps a datastore.BulkWriteRelationshipSource, recording
// a copy of each relationship it yields so the changelog can be populated
// after the underlying COPY completes.
//
// datastore.BulkWriteRelationshipSource documents that sources "may re-use
// the same memory address for every tuple" across calls to Next, so this
// wrapper cannot simply retain the returned pointer: it copies the
// Relationship value and, defensively, deep-copies the caveat proto (the one
// nested pointer a source implementation might otherwise mutate/reuse
// in-place) before storing it.
type capturingBulkSource struct {
	inner datastore.BulkWriteRelationshipSource
	seen  []tuple.Relationship
}

func (c *capturingBulkSource) Next(ctx context.Context) (*tuple.Relationship, error) {
	rel, err := c.inner.Next(ctx)
	if err != nil {
		return nil, err
	}
	if rel != nil {
		captured := *rel
		if rel.OptionalCaveat != nil {
			captured.OptionalCaveat = proto.Clone(rel.OptionalCaveat).(*core.ContextualizedCaveat)
		}
		c.seen = append(c.seen, captured)
	}
	return rel, nil
}

// appendRelationshipChangelogBatch inserts one 'create' changelog row per
// relationship in a single multi-row INSERT, all stamped with the
// transaction's cluster_logical_timestamp(). Each row gets a unique ordinal
// from rwt.nextChangelogOrdinal() so it cannot collide with any other
// changelog row (relationship, schema, or another bulk batch) appended
// within the same transaction.
func (rwt *crdbReadWriteTXN) appendRelationshipChangelogBatch(ctx context.Context, rels []tuple.Relationship, ttlExpiration time.Time) error {
	if len(rels) == 0 {
		return nil
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
	)
	for _, rel := range rels {
		var caveatName string
		var caveatContext map[string]any
		if rel.OptionalCaveat != nil {
			caveatName = rel.OptionalCaveat.CaveatName
			caveatContext = rel.OptionalCaveat.Context.AsMap()
		}
		insert = insert.Values(
			sq.Expr("cluster_logical_timestamp()"),
			rwt.nextChangelogOrdinal(),
			"rel",
			rel.Resource.ObjectType, rel.Resource.ObjectID, rel.Resource.Relation,
			rel.Subject.ObjectType, rel.Subject.ObjectID, rel.Subject.Relation,
			caveatName, caveatContext, rel.OptionalExpiration,
			"create",
			ttlExpiration,
		)
	}
	sql, args, err := insert.ToSql()
	if err != nil {
		return fmt.Errorf("unable to build bulk changelog insert: %w", err)
	}
	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("unable to write bulk changelog rows: %w", err)
	}
	return nil
}
