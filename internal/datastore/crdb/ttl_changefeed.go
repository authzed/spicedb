package crdb

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	log "github.com/authzed/spicedb/internal/logging"
)

const (
	queryShowCreateTable                 = "SELECT create_statement FROM [SHOW CREATE TABLE %s]"
	queryDisableTTLChangefeedReplication = "ALTER TABLE %s SET (ttl_disable_changefeed_replication = 'true')"
)

// supportsTTLChangefeedReplicationParam returns whether the connected CockroachDB
// version supports the ttl_disable_changefeed_replication table storage parameter.
// The parameter was added in CockroachDB v24.1.0 and prevents deletes performed by
// the row-level TTL job from being emitted to changefeeds. We gate on >= 24.1.0 GA,
// which also includes the fix for cockroachdb/cockroach#121589 (TTL deletes leaking
// past the filter when the TTL job's transaction is retried).
func supportsTTLChangefeedReplicationParam(version crdbVersion) bool {
	return version.Major > 24 || (version.Major == 24 && version.Minor >= 1)
}

// ensureTTLChangefeedReplicationDisabled checks that the relationship tables have
// ttl_disable_changefeed_replication set, and sets it if not. This prevents CRDB's
// row-level TTL job (which hard-deletes expired relationships) from emitting those
// deletes into the changefeed that backs the Watch API; CRDB provides no per-event
// origin marker, so suppression at the source is the only robust mechanism.
//
// This is fail-open by design: the runtime credentials may lack ALTER TABLE
// privileges, and datastore startup must not fail because of it. Any error is
// logged loudly and startup continues.
//
// NOTE: a future changefeed wishing to receive the TTL deletes (e.g. to report
// row GCs as events) can opt back in via the ignore_disable_changefeed_replication
// changefeed option.
func ensureTTLChangefeedReplicationDisabled(ctx context.Context, conn pgxcommon.DBFuncQuerier, version crdbVersion) {
	if !supportsTTLChangefeedReplicationParam(version) {
		log.Ctx(ctx).Warn().
			Object("version", version).
			Msg("this version of CockroachDB does not support the ttl_disable_changefeed_replication storage parameter (added in v24.1); deletions of expired relationships performed by CockroachDB's row-level TTL job will be emitted by the Watch API")
		return
	}

	for _, tableName := range []string{schema.TableTuple, schema.TableTupleWithIntegrity} {
		var createStatement string
		if err := conn.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
			return row.Scan(&createStatement)
		}, fmt.Sprintf(queryShowCreateTable, tableName)); err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("table", tableName).
				Msg("could not check for the ttl_disable_changefeed_replication storage parameter")
			continue
		}

		// NOTE: presence of the parameter (regardless of value) is respected: if an
		// operator has explicitly configured it, we do not override their choice.
		if strings.Contains(createStatement, "ttl_disable_changefeed_replication") {
			continue
		}

		if err := conn.ExecFunc(ctx, func(ctx context.Context, tag pgconn.CommandTag, err error) error {
			return err
		}, fmt.Sprintf(queryDisableTTLChangefeedReplication, tableName)); err != nil {
			log.Ctx(ctx).Warn().Err(err).Str("table", tableName).
				Msg("could not set ttl_disable_changefeed_replication; deletions of expired relationships performed by CockroachDB's row-level TTL job will be emitted by the Watch API. To fix, run manually: ALTER TABLE " + tableName + " SET (ttl_disable_changefeed_replication = 'true')")
			continue
		}

		log.Ctx(ctx).Info().Str("table", tableName).
			Msg("set ttl_disable_changefeed_replication to suppress row-level TTL deletes from the Watch API changefeed")
	}
}
