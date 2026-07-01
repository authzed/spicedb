package crdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
)

func TestCreateChangelogTableSQL(t *testing.T) {
	sql := createChangelogTableSQL(24 * time.Hour)
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS "+schema.TableRelationshipChangelog)
	require.Contains(t, sql, "PRIMARY KEY ("+schema.ColChangeTS+", "+schema.ColChangeOrdinal+") USING HASH")
	require.Contains(t, sql, "ttl_expiration_expression")
	require.Contains(t, sql, schema.ColChangeTTLExpiration)
	require.Contains(t, sql, "ttl_disable_changefeed_replication")
}
