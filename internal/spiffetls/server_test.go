package spiffetls

import (
	"crypto/tls"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testutil/spiffetest"
)

const spicedbID = "spiffe://spicedb.test/spicedb"

// serverFixture is a server trust domain, a client in that trust domain, and a
// client in another, federated one, each with its own Workload API.
type serverFixture struct {
	ownCA, otherCA *spiffetest.CA
	serverAPI      *spiffetest.WorkloadAPI
}

func newServerFixture(t *testing.T) *serverFixture {
	t.Helper()
	f := &serverFixture{
		ownCA:     spiffetest.NewCA(t, "spicedb.test"),
		otherCA:   spiffetest.NewCA(t, "other.test"),
		serverAPI: spiffetest.NewWorkloadAPI(t),
	}
	f.serverAPI.SetX509SVIDs(t, []*x509svid.SVID{f.ownCA.MintX509SVID(t, spicedbID)}, f.ownCA, f.otherCA)
	return f
}

func (f *serverFixture) newServer(t *testing.T, allowed ...string) *Source {
	t.Helper()
	source, err := NewServer(t.Context(), ServerOptions{Enabled: true, Socket: f.serverAPI.Addr, ID: spicedbID, AllowedIDs: allowed})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	return source
}

// client returns a client configuration presenting an SVID for id, which must
// be in one of the fixture's trust domains, and accepting the server by ID.
func (f *serverFixture) client(t *testing.T, id string) *tls.Config {
	t.Helper()

	ca := f.ownCA
	if !spiffeid.RequireFromString(id).MemberOf(f.ownCA.TrustDomain) {
		ca = f.otherCA
	}
	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{ca.MintX509SVID(t, id)}, f.ownCA, f.otherCA)

	source, err := New(t.Context(), Options{Enabled: true, Socket: api.Addr, ID: id, ServerID: spicedbID})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	return source.ClientConfig()
}

// serve completes one handshake with serverCfg against a client using
// clientCfg, and returns the errors of both sides.
func serve(t *testing.T, serverCfg, clientCfg *tls.Config) error {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		serverDone <- tls.Server(conn, serverCfg).HandshakeContext(t.Context())
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	client := tls.Client(conn, clientCfg)
	clientErr := client.HandshakeContext(t.Context())
	if clientErr == nil {
		// In TLS 1.3 the server verifies the client after the client has
		// finished, so read to receive a rejection.
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			clientErr = err
		}
	}
	_ = conn.Close()

	return errors.Join(<-serverDone, clientErr)
}

func TestServerOptionsValidate(t *testing.T) {
	socket := "unix:///run/spire/agent.sock"

	for _, tc := range []struct {
		name    string
		opts    ServerOptions
		wantErr string
	}{
		{name: "unset", opts: ServerOptions{}},
		{name: "enabled with a socket", opts: ServerOptions{Enabled: true, Socket: socket}},
		{name: "allowed IDs and trust domains", opts: ServerOptions{Enabled: true, Socket: socket, AllowedIDs: []string{"spiffe://other.test/app", "spiffe://spicedb.test"}}},
		{name: "allowed IDs without enabling", opts: ServerOptions{AllowedIDs: []string{"spiffe://other.test/app"}}, wantErr: "set --grpc-spiffe"},
		{name: "prefix names the flags", opts: ServerOptions{FlagPrefix: "dispatch-cluster", Socket: socket}, wantErr: "set --dispatch-cluster-spiffe"},
		{name: "enabled without any socket", opts: ServerOptions{Enabled: true}, wantErr: "set --grpc-spiffe-socket or SPIFFE_ENDPOINT_SOCKET"},
		{name: "malformed allowed ID", opts: ServerOptions{Enabled: true, Socket: socket, AllowedIDs: []string{"other.test/app"}}, wantErr: "invalid grpc allowed SPIFFE ID"},
		{name: "trust domain as own ID", opts: ServerOptions{Enabled: true, Socket: socket, ID: "spiffe://spicedb.test"}, wantErr: "a trust domain is not the ID of an SVID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SPIFFE_ENDPOINT_SOCKET", "")

			err := tc.opts.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestServerAllowsItsOwnTrustDomainByDefault(t *testing.T) {
	f := newServerFixture(t)
	server := f.newServer(t).ServerConfig()

	require.NoError(t, serve(t, server, f.client(t, "spiffe://spicedb.test/app")))
	require.ErrorContains(t, serve(t, server, f.client(t, "spiffe://other.test/app")), "is not allowed to connect")
}

func TestServerAllowedIDs(t *testing.T) {
	f := newServerFixture(t)
	server := f.newServer(t, "spiffe://spicedb.test/api", "spiffe://other.test").ServerConfig()

	t.Run("listed ID", func(t *testing.T) {
		require.NoError(t, serve(t, server, f.client(t, "spiffe://spicedb.test/api")))
	})

	t.Run("listed trust domain", func(t *testing.T) {
		require.NoError(t, serve(t, server, f.client(t, "spiffe://other.test/anything")))
	})

	t.Run("unlisted ID in the server's own trust domain", func(t *testing.T) {
		require.ErrorContains(t, serve(t, server, f.client(t, "spiffe://spicedb.test/other")), "is not allowed to connect")
	})
}

func TestServerRequiresAClientCertificate(t *testing.T) {
	f := newServerFixture(t)
	server := f.newServer(t).ServerConfig()

	noCert := f.client(t, "spiffe://spicedb.test/app").Clone()
	noCert.GetClientCertificate = nil
	require.Error(t, serve(t, server, noCert))
}

func TestServerAcceptsItself(t *testing.T) {
	f := newServerFixture(t)
	// Allowing only another workload must not lock the server out of its own
	// listener.
	source := f.newServer(t, "spiffe://other.test/app")

	require.NoError(t, serve(t, source.ServerConfig(), source.SelfClientConfig()))
}

func TestSelfClientAcceptsOnlyItself(t *testing.T) {
	f := newServerFixture(t)
	self := f.newServer(t).SelfClientConfig()

	// Another server in the same trust domain, with its own Workload API.
	api := spiffetest.NewWorkloadAPI(t)
	api.SetX509SVIDs(t, []*x509svid.SVID{f.ownCA.MintX509SVID(t, "spiffe://spicedb.test/impostor")}, f.ownCA)
	impostor, err := NewServer(t.Context(), ServerOptions{Enabled: true, Socket: api.Addr})
	require.NoError(t, err)
	t.Cleanup(func() { _ = impostor.Close() })

	// The impostor would accept the client, which is in its trust domain; the
	// client must refuse the impostor.
	require.ErrorContains(t, serve(t, impostor.ServerConfig(), self), "unexpected ID")
}

func TestServerRotation(t *testing.T) {
	f := newServerFixture(t)
	server := f.newServer(t).ServerConfig()
	client := f.client(t, "spiffe://spicedb.test/app")
	require.NoError(t, serve(t, server, client))

	// Rotate the server to an SVID from an authority clients don't trust:
	// handshakes must start failing without rebuilding the configuration.
	untrusted := spiffetest.NewCA(t, "spicedb.test")
	f.serverAPI.SetX509SVIDs(t, []*x509svid.SVID{untrusted.MintX509SVID(t, spicedbID)}, untrusted, f.otherCA)
	require.Eventually(t, func() bool { return serve(t, server, client) != nil }, 5*time.Second, 10*time.Millisecond)
}
