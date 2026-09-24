//go:build datastore

package crdb

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/cmd/datastore/dsconfig"
)

// The shipped production defaults, which are what these tests are about.
var (
	prodWriteConns         = int32(dsconfig.DefaultWriteConnPool().MaxOpenConns)
	prodConnectRate        = dsconfig.DefaultDatastoreConfig().ConnectRate
	prodAcquisitionTimeout = dsconfig.DefaultDatastoreConfig().WriteAcquisitionTimeout
)

// newProductionWritePool builds a write pool configured the way a running
// SpiceDB configures one, and does not wait for it to fill.
func newProductionWritePool(t *testing.T, uri string) *pool.RetryPool {
	t.Helper()

	config, err := pgxpool.ParseConfig(uri)
	require.NoError(t, err)
	config.MinConns = prodWriteConns
	config.MaxConns = prodWriteConns

	healthTracker, err := pool.NewNodeHealthChecker(uri)
	require.NoError(t, err)

	writePool, err := pool.NewRetryPool(context.Background(), "write", config, healthTracker, 5, prodConnectRate)
	require.NoError(t, err)
	t.Cleanup(writePool.Close)

	return writePool
}

// TestColdWritePoolServesItsFirstWrite covers a write that arrives before the
// pool has finished opening its connections, which is where a freshly started
// SpiceDB instance is.
//
// Nothing is exhausted here: the pool is at its full size but every connection
// in it is still being opened behind the connect-rate limiter, one per
// --datastore-connect-rate. The write acquisition timeout is backpressure
// against a pool whose connections are all in use, and must not fail this.
func TestColdWritePoolServesItsFirstWrite(t *testing.T) {
	t.Parallel()

	engine := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	uri := engine.NewDatabase(t)

	// Repeated because the failure it guards against is a race between the
	// write and the pool filling, and a single run can win that race.
	for range 5 {
		writePool := newProductionWritePool(t, uri)

		require.NoError(t, writePool.TryBeginFunc(t.Context(), prodAcquisitionTimeout,
			func(tx pgx.Tx) error { return nil }),
			"a write pool that is still opening its connections is not an exhausted one")
	}
}

// TestWritePoolServesAWriteWhileReplacingConnections covers the same thing
// after the pool has lost the connections it had, which is what the node
// connection balancer does to every connection on a node it has marked
// unhealthy. The pool has to open them again at the connect rate, and an
// otherwise idle server must not fail its writes while it does.
func TestWritePoolServesAWriteWhileReplacingConnections(t *testing.T) {
	t.Parallel()

	engine := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	uri := engine.NewDatabase(t)

	writePool := newProductionWritePool(t, uri)
	require.Eventually(t, func() bool { return writePool.Stat().IdleConns() >= prodWriteConns },
		time.Minute, 25*time.Millisecond, "write pool never filled")

	// Exactly what NodeConnectionBalancer does with the connections to a node
	// it considers unhealthy.
	conns := writePool.AcquireAllIdle(t.Context())
	require.NotEmpty(t, conns)
	for _, conn := range conns {
		writePool.GC(conn.Conn())
	}
	for _, conn := range conns {
		conn.Release()
	}

	require.NoError(t, writePool.TryBeginFunc(t.Context(), prodAcquisitionTimeout,
		func(tx pgx.Tx) error { return nil }),
		"a write pool that is replacing lost connections is not an exhausted one")
}
