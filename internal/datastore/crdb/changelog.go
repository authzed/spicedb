package crdb

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
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
