//go:build ci
// +build ci

package services_test

import (
	"context"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/internal/testserver"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/internal/testserver/datastore/config"
	dsconfig "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestHealthCheck(t *testing.T) {
	require := require.New(t)

	for _, engine := range datastore.Engines {
		b := testdatastore.RunDatastoreEngine(t, engine)
		t.Run(engine, func(t *testing.T) {
			t.Logf("Running %s health check test", engine)
			ds := b.NewDatastore(t, config.DatastoreConfigInitFunc(t,
				dsconfig.WithWatchBufferLength(0),
				dsconfig.WithGCWindow(time.Duration(90_000_000_000_000)),
				dsconfig.WithRevisionQuantization(10)))
			ds, _ = tf.StandardDatastoreWithData(ds, require)

			dispatchConns, cleanup := testserver.TestClusterWithDispatch(t, 2, ds)
			t.Cleanup(cleanup)

			zerolog.SetGlobalLevel(zerolog.Disabled)

			runHealthChecks(require, dispatchConns[0])
		})
	}

	// Check server without dispatching
	conn, cleanup, _, _ := testserver.NewTestServer(require, 0, memdb.DisableGC, true, tf.StandardDatastoreWithData)
	t.Cleanup(cleanup)
	runHealthChecks(require, conn)
}

func runHealthChecks(require *require.Assertions, conn *grpc.ClientConn) {
	hclient := healthpb.NewHealthClient(conn)

	require.Eventually(func() bool {
		resp, err := hclient.Check(context.Background(), &healthpb.HealthCheckRequest{Service: v1.PermissionsService_ServiceDesc.ServiceName})
		require.NoError(err)
		return healthpb.HealthCheckResponse_SERVING == resp.GetStatus()
	}, 5*time.Second, 100*time.Millisecond)
}
