//go:build datastore && postgres

package postgres

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// The pool sizes used by these tests. They match the shipped defaults
// (--datastore-conn-pool-read-min-open / --datastore-conn-pool-write-min-open),
// because the behaviour under test is what an operator gets out of the box.
const (
	warmupReadConns  = 20
	warmupWriteConns = 10
)

// TestDatastoreReturnsWithWarmPools is a regression test for a pod that
// Kubernetes marks Ready while its connection pools are still empty.
//
// pgxpool.NewWithConfig does not block: it starts a goroutine to open MinConns
// connections and returns to the caller straight away. Before this was fixed,
// newPostgresDatastore inherited that behaviour, so a freshly rolled SpiceDB
// pod started serving traffic with nothing in its pools and every request in
// the first burst paid connection establishment inline -- a thundering herd on
// the database at exactly the moment a rollout is shifting traffic onto the new
// pod.
//
// The test asserts on pool statistics sampled immediately on return from the
// constructor, with no waiting and no retry loop: that is the whole point.
func TestDatastoreReturnsWithWarmPools(t *testing.T) {
	t.Parallel()

	engine := testdatastore.RunPostgresForTesting(t, postgresTestVersion(), false)
	ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := newPostgresDatastore(t.Context(), uri, primaryInstanceID,
			ReadConnsMinOpen(warmupReadConns),
			ReadConnsMaxOpen(warmupReadConns),
			WriteConnsMinOpen(warmupWriteConns),
			WriteConnsMaxOpen(warmupWriteConns),
			GCInterval(disableBackgroundGC),
			WithRevisionHeartbeat(false),
		)
		require.NoError(t, err)
		return ds
	})

	pgDS, ok := ds.(*pgDatastore)
	require.True(t, ok, "expected the concrete postgres datastore, got %T", ds)

	readPool, ok := pgDS.readPool.(*pgxpool.Pool)
	require.True(t, ok, "expected an unwrapped pgxpool for the read pool, got %T", pgDS.readPool)
	writePool, ok := pgDS.writePool.(*pgxpool.Pool)
	require.True(t, ok, "expected an unwrapped pgxpool for the write pool, got %T", pgDS.writePool)

	readStat := readPool.Stat()
	require.GreaterOrEqual(t, readStat.TotalConns(), int32(warmupReadConns),
		"read pool has %d of %d connections established on return from the constructor",
		readStat.TotalConns(), warmupReadConns)
	require.GreaterOrEqual(t, readStat.IdleConns(), int32(warmupReadConns),
		"read pool has %d of %d connections idle and ready to serve on return from the constructor",
		readStat.IdleConns(), warmupReadConns)

	writeStat := writePool.Stat()
	require.GreaterOrEqual(t, writeStat.TotalConns(), int32(warmupWriteConns),
		"write pool has %d of %d connections established on return from the constructor",
		writeStat.TotalConns(), warmupWriteConns)
	require.GreaterOrEqual(t, writeStat.IdleConns(), int32(warmupWriteConns),
		"write pool has %d of %d connections idle and ready to serve on return from the constructor",
		writeStat.IdleConns(), warmupWriteConns)
}

// TestDatastoreStartsWhenMinConnsExceedsMaxConns pins down the clamp in
// WarmupPool. A minimum above the maximum is a configuration SpiceDB accepts
// today -- ConfigurePgx only logs a warning -- but the maximum is the hard size
// limit of the underlying pool, so a warm-up that waited for the raw minimum
// would wait for the full timeout and then refuse to start a server that was
// perfectly able to serve traffic.
func TestDatastoreStartsWhenMinConnsExceedsMaxConns(t *testing.T) {
	t.Parallel()

	const maxConns = 5

	engine := testdatastore.RunPostgresForTesting(t, postgresTestVersion(), false)
	startedAt := time.Now()
	ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := newPostgresDatastore(t.Context(), uri, primaryInstanceID,
			ReadConnsMinOpen(warmupReadConns),
			ReadConnsMaxOpen(maxConns),
			WriteConnsMinOpen(warmupWriteConns),
			WriteConnsMaxOpen(maxConns),
			GCInterval(disableBackgroundGC),
			WithRevisionHeartbeat(false),
		)
		require.NoError(t, err)
		return ds
	})

	require.Less(t, time.Since(startedAt), pgxcommon.PoolWarmupTimeout,
		"warming up to an unreachable minimum waited for the whole timeout instead of clamping to the maximum")

	readPool, ok := ds.(*pgDatastore).readPool.(*pgxpool.Pool)
	require.True(t, ok)
	require.GreaterOrEqual(t, readPool.Stat().IdleConns(), int32(maxConns))
}
