package server

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/spiffetls"
	"github.com/authzed/spicedb/internal/testutil/spiffetest"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/testutil"
)

const (
	spiffeServerID  = "spiffe://spicedb.test/spicedb"
	spiffeAllowedID = "spiffe://spicedb.test/api"
)

// TestServerSPIFFE serves the gRPC API over mutual TLS with an SVID from the
// Workload API, allowing a single client SPIFFE ID, and checks who can
// connect. The REST gateway reaches the API with the server's own SVID, so it
// keeps working although it is not in the allowed list.
func TestServerSPIFFE(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	ca := spiffetest.NewCA(t, "spicedb.test")
	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{ca.MintX509SVID(t, spiffeServerID)}, ca)

	grpcAddr, httpAddr := freeAddr(t), freeAddr(t)

	ctx, cancel := context.WithCancel(t.Context())
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	rs, err := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{
			Network:          "tcp",
			Address:          grpcAddr,
			Enabled:          true,
			SPIFFE:           true,
			SPIFFESocket:     api.Addr,
			SPIFFEAllowedIDs: []string{spiffeAllowedID},
		}),
		WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: true, HTTPAddress: httpAddr}),
		WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		WithEnableMemoryProtectionMiddleware(false),
	).Complete(ctx)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_ = rs.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	t.Run("allowed client", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.NoError(c, healthCheck(t, grpcAddr, clientCredentials(t, ca, spiffeAllowedID)))
		}, 10*time.Second, 50*time.Millisecond)
	})

	t.Run("client not in the allowed list", func(t *testing.T) {
		require.Error(t, healthCheck(t, grpcAddr, clientCredentials(t, ca, "spiffe://spicedb.test/broker")))
	})

	t.Run("plaintext client", func(t *testing.T) {
		require.Error(t, healthCheck(t, grpcAddr, insecure.NewCredentials()))
	})

	t.Run("REST gateway", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := http.Get("http://" + httpAddr + "/healthz") //nolint:noctx // a test against a local server
			if !assert.NoError(c, err) {
				return
			}
			defer resp.Body.Close()
			assert.Equal(c, http.StatusOK, resp.StatusCode, "the gateway should reach the gRPC API with the server's own SVID")
		}, 10*time.Second, 50*time.Millisecond)
	})
}

// clientCredentials presents an SVID for id, minted by ca, and accepts only
// the server's SPIFFE ID.
func clientCredentials(t *testing.T, ca *spiffetest.CA, id string) credentials.TransportCredentials {
	t.Helper()

	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{ca.MintX509SVID(t, id)}, ca)

	source, err := spiffetls.New(t.Context(), spiffetls.Options{Enabled: true, Socket: api.Addr, ID: id, ServerID: spiffeServerID})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	return credentials.NewTLS(source.ClientConfig())
}

func healthCheck(t *testing.T, addr string, creds credentials.TransportCredentials) error {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(false))
	return err
}

// freeAddr returns a loopback address with a port that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

func TestServerRejectsDispatchSPIFFE(t *testing.T) {
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	_, err = ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{Network: util.BufferedNetwork, Enabled: true}),
		WithDispatchServer(util.GRPCServerConfig{Enabled: true, SPIFFE: true}),
	).Complete(t.Context())
	require.ErrorContains(t, err, "--dispatch-cluster-spiffe is not supported yet")
}
