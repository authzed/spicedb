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
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/grpcutil"

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

// TestDispatchSPIFFE runs a node that dispatches to itself over the cluster
// port, with both ends using SPIFFE: the dispatch server requires a client
// SVID and the dispatch client verifies the server by SPIFFE ID. Every check
// crosses that handshake.
func TestDispatchSPIFFE(t *testing.T) {
	// As cmd/spicedb does; registering is idempotent.
	balancer.Register(ConsistentHashringBuilder)
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	ca := spiffetest.NewCA(t, "spicedb.test")
	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{ca.MintX509SVID(t, spiffeServerID)}, ca)

	for _, tc := range []struct {
		name     string
		serverID string
		wantErr  bool
	}{
		{name: "peer with the expected ID", serverID: spiffeServerID},
		{name: "peer in the expected trust domain", serverID: "spiffe://spicedb.test"},
		{name: "peer with another ID", serverID: "spiffe://spicedb.test/other", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			permissions := runDispatchingServer(t, api.Addr, tc.serverID)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			resp, err := permissions.CheckPermission(ctx, &v1.CheckPermissionRequest{
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
				Resource:    &v1.ObjectReference{ObjectType: "document", ObjectId: "doc"},
				Permission:  "view",
				Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "alice"}},
			})
			if tc.wantErr {
				require.Error(t, err, "the dispatch client must refuse a peer with another SPIFFE ID")
				return
			}
			require.NoError(t, err)
			require.Equal(t, v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION, resp.Permissionship)
		})
	}
}

// runDispatchingServer starts a node whose API is in memory and whose
// dispatches go to its own cluster port over SPIFFE mutual TLS, expecting the
// upstream to present serverID. It writes a schema and one relationship.
func runDispatchingServer(t *testing.T, socket, serverID string) v1.PermissionsServiceClient {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	ds, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 1*time.Second, 10*time.Second)
	require.NoError(t, err)

	dispatchAddr := freeAddr(t)
	rs, err := ConfigWithOptions(
		&Config{},
		WithPresharedSecureKey("psk"),
		WithDatastore(ds),
		WithGRPCServer(util.GRPCServerConfig{Network: util.BufferedNetwork, Enabled: true}),
		WithDispatchServer(util.GRPCServerConfig{
			Network:      "tcp",
			Address:      dispatchAddr,
			Enabled:      true,
			SPIFFE:       true,
			SPIFFESocket: socket,
		}),
		WithDispatchUpstreamAddr(dispatchAddr),
		WithDispatchUpstreamSPIFFE(true),
		WithDispatchUpstreamSPIFFESocket(socket),
		WithDispatchUpstreamSPIFFEServerID(serverID),
		WithDispatchCacheConfig(CacheConfig{Enabled: false}),
		WithClusterDispatchCacheConfig(CacheConfig{Enabled: false}),
		WithNamespaceCacheConfig(CacheConfig{Enabled: false}),
		WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: false}),
		WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		WithSilentlyDisableTelemetry(true),
		WithEnableMemoryProtectionMiddleware(false),
	).Complete(ctx)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_ = rs.Run(ctx)
		close(done)
	}()

	conn, err := rs.NewClient(grpcutil.WithInsecureBearerToken("psk"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
		cancel()
		<-done
	})

	_, err = v1.NewSchemaServiceClient(conn).WriteSchema(ctx, &v1.WriteSchemaRequest{Schema: `
definition user {}

definition document {
	relation viewer: user
	permission view = viewer
}`})
	require.NoError(t, err)

	permissions := v1.NewPermissionsServiceClient(conn)
	_, err = permissions.WriteRelationships(ctx, &v1.WriteRelationshipsRequest{Updates: []*v1.RelationshipUpdate{{
		Operation: v1.RelationshipUpdate_OPERATION_TOUCH,
		Relationship: &v1.Relationship{
			Resource: &v1.ObjectReference{ObjectType: "document", ObjectId: "doc"},
			Relation: "viewer",
			Subject:  &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "alice"}},
		},
	}}})
	require.NoError(t, err)

	return permissions
}
