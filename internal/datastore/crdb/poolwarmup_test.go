//go:build datastore

package crdb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

// The pool sizes used by this test. They are smaller than the shipped defaults
// because every CockroachDB connection passes through a rate limiter of one
// connection per --datastore-connect-rate (100ms), so a default-sized pool
// would add several seconds to the test for no extra coverage.
const (
	crdbWarmupReadConns  = 10
	crdbWarmupWriteConns = 5
)

// TestDatastoreReturnsWithWarmPools is the CockroachDB counterpart of the
// Postgres test of the same name. The CockroachDB datastore wraps pgxpool in
// its own RetryPool, but RetryPool is built on pgxpool.NewWithConfig, which
// returns before any connection exists and fills the pool from a background
// goroutine. Without the warm-up in NewRetryPool, a freshly rolled pod is
// marked Ready by Kubernetes with empty read and write pools and the first
// burst of traffic pays connection establishment inline.
//
// The pool statistics are sampled immediately on return from the constructor,
// with no waiting and no retry loop.
func TestDatastoreReturnsWithWarmPools(t *testing.T) {
	t.Parallel()

	engine := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
		ds, err := newCRDBDatastore(t.Context(), uri,
			ReadConnsMinOpen(crdbWarmupReadConns),
			ReadConnsMaxOpen(crdbWarmupReadConns),
			WriteConnsMinOpen(crdbWarmupWriteConns),
			WriteConnsMaxOpen(crdbWarmupWriteConns),
			WithAcquireTimeout(5*time.Second),
		)
		require.NoError(t, err)
		return ds
	})
	t.Cleanup(func() { _ = ds.Close() })

	crdbDS, ok := ds.(*crdbDatastore)
	require.True(t, ok, "expected the concrete cockroachdb datastore, got %T", ds)

	readStat := crdbDS.readPool.Stat()
	require.GreaterOrEqual(t, readStat.TotalConns(), int32(crdbWarmupReadConns),
		"read pool has %d of %d connections established on return from the constructor",
		readStat.TotalConns(), crdbWarmupReadConns)
	require.GreaterOrEqual(t, readStat.IdleConns(), int32(crdbWarmupReadConns),
		"read pool has %d of %d connections idle and ready to serve on return from the constructor",
		readStat.IdleConns(), crdbWarmupReadConns)

	writeStat := crdbDS.writePool.Stat()
	require.GreaterOrEqual(t, writeStat.TotalConns(), int32(crdbWarmupWriteConns),
		"write pool has %d of %d connections established on return from the constructor",
		writeStat.TotalConns(), crdbWarmupWriteConns)
	require.GreaterOrEqual(t, writeStat.IdleConns(), int32(crdbWarmupWriteConns),
		"write pool has %d of %d connections idle and ready to serve on return from the constructor",
		writeStat.IdleConns(), crdbWarmupWriteConns)
}
