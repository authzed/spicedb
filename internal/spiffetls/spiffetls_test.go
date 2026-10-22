package spiffetls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/internal/testutil/spiffetest"
	sdbtestutil "github.com/authzed/spicedb/pkg/testutil"
)

const (
	clientID = "spiffe://client.test/spicedb"
	serverID = "spiffe://server.test/mysql"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, sdbtestutil.GoLeakIgnores()...)
}

// fixture is a client trust domain, whose SVIDs are signed by an intermediate,
// and a separate server trust domain, federated with it.
type fixture struct {
	api      *spiffetest.WorkloadAPI
	clientCA *spiffetest.CA
	serverCA *spiffetest.CA
	server   *x509svid.SVID
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	serverCA := spiffetest.NewCA(t, "server.test")
	return &fixture{
		api:      spiffetest.NewWorkloadAPI(t),
		clientCA: spiffetest.NewCA(t, "client.test").Intermediate(t),
		serverCA: serverCA,
		// No DNS name: the server is authenticated by its SPIFFE ID alone.
		server: serverCA.MintX509SVID(t, serverID),
	}
}

func (f *fixture) options() Options {
	return Options{Enabled: true, Socket: f.api.Addr, ID: clientID, ServerID: serverID}
}

func (f *fixture) publish(t *testing.T, svids ...*x509svid.SVID) {
	t.Helper()
	f.api.SetX509SVIDs(t, svids, f.clientCA, f.serverCA)
}

func (f *fixture) newSource(t *testing.T) *Source {
	t.Helper()
	source, err := New(t.Context(), f.options())
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })
	return source
}

// handshake connects with cfg to a server presenting serverSVID and requiring
// a client certificate from the client trust domain, and returns the chain the
// client presented.
func (f *fixture) handshake(t *testing.T, cfg *tls.Config, serverSVID *x509svid.SVID) ([]*x509.Certificate, error) {
	t.Helper()

	clientRoots := x509.NewCertPool()
	for _, root := range f.clientCA.Bundle() {
		clientRoots.AddCert(root)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	type result struct {
		presented []*x509.Certificate
		err       error
	}
	serverDone := make(chan result, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- result{err: err}
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

		server := tls.Server(conn, &tls.Config{
			Certificates: []tls.Certificate{spiffetest.TLSCertificate(serverSVID)},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientRoots,
			MinVersion:   tls.VersionTLS12,
		})
		err = server.HandshakeContext(t.Context())
		serverDone <- result{presented: server.ConnectionState().PeerCertificates, err: err}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	client := tls.Client(conn, cfg)
	clientErr := client.HandshakeContext(t.Context())
	if clientErr == nil {
		// In TLS 1.3 the server verifies the client certificate after the
		// client has finished, so read to receive a rejection. A server that
		// accepted simply closes the connection.
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
			clientErr = err
		}
	}
	_ = conn.Close()

	server := <-serverDone
	if clientErr != nil {
		return nil, clientErr
	}
	if server.err != nil {
		return nil, server.err
	}
	return server.presented, nil
}

func TestOptionsValidate(t *testing.T) {
	socket := "unix:///run/spire/agent.sock"

	for _, tc := range []struct {
		name    string
		env     string
		opts    Options
		wantErr string
	}{
		{name: "unset", opts: Options{}},
		{name: "enabled with the socket from the environment", env: socket, opts: Options{Enabled: true}},
		{name: "enabled with a socket", opts: Options{Enabled: true, Socket: socket}},
		{name: "all set", opts: Options{Enabled: true, Socket: socket, ID: clientID, ServerID: serverID}},
		{name: "server trust domain", opts: Options{Enabled: true, Socket: socket, ServerID: "spiffe://server.test"}},
		{name: "enabled without any socket", opts: Options{Enabled: true}, wantErr: "set --datastore-spiffe-socket or SPIFFE_ENDPOINT_SOCKET"},
		{name: "socket without enabling", opts: Options{Socket: socket}, wantErr: "set --datastore-spiffe"},
		{name: "ID without enabling", env: socket, opts: Options{ID: clientID}, wantErr: "set --datastore-spiffe"},
		{name: "server ID without enabling", opts: Options{ServerID: serverID}, wantErr: "set --datastore-spiffe"},
		{name: "socket without scheme", opts: Options{Enabled: true, Socket: "/run/spire/agent.sock"}, wantErr: "invalid datastore SPIFFE socket"},
		{name: "environment socket without scheme", env: "/run/spire/agent.sock", opts: Options{Enabled: true}, wantErr: "in SPIFFE_ENDPOINT_SOCKET"},
		{name: "malformed ID", opts: Options{Enabled: true, Socket: socket, ID: "spicedb"}, wantErr: "invalid datastore SPIFFE ID"},
		{name: "trust domain as ID", opts: Options{Enabled: true, Socket: socket, ID: "spiffe://client.test"}, wantErr: "a trust domain is not the ID of an SVID"},
		{name: "malformed server ID", opts: Options{Enabled: true, Socket: socket, ServerID: "https://server.test/mysql"}, wantErr: "invalid datastore server SPIFFE ID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SPIFFE_ENDPOINT_SOCKET", tc.env)

			err := tc.opts.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestSocketFromTheEnvironment(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, clientID))
	t.Setenv("SPIFFE_ENDPOINT_SOCKET", f.api.Addr)

	source, err := New(t.Context(), Options{Enabled: true, ServerID: serverID})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	_, err = f.handshake(t, source.ClientConfig(), f.server)
	require.NoError(t, err)
}

func TestNotEnabled(t *testing.T) {
	_, err := New(t.Context(), Options{Socket: "unix:///run/spire/agent.sock"})
	require.ErrorContains(t, err, "not enabled")
}

func TestMutualAuthentication(t *testing.T) {
	f := newFixture(t)
	svid := f.clientCA.MintX509SVID(t, clientID, "spicedb")
	f.publish(t, svid)

	presented, err := f.handshake(t, f.newSource(t).ClientConfig(), f.server)
	require.NoError(t, err)

	require.Equal(t, svid.Certificates[0].Raw, presented[0].Raw, "the client should present its SVID")
	require.Len(t, presented, 2, "the client should send the intermediate along with its leaf")
	require.Equal(t, "spicedb", presented[0].Subject.CommonName)
}

func TestRotationTakesEffectOnTheNextHandshake(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, clientID))

	cfg := f.newSource(t).ClientConfig()
	_, err := f.handshake(t, cfg, f.server)
	require.NoError(t, err)

	rotated := f.clientCA.MintX509SVID(t, clientID)
	f.publish(t, rotated)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		presented, err := f.handshake(t, cfg, f.server)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, rotated.Certificates[0].Raw, presented[0].Raw)
	}, 5*time.Second, 10*time.Millisecond, "the same configuration should present the rotated SVID")
}

func TestPicksTheSVIDByID(t *testing.T) {
	f := newFixture(t)
	other := f.clientCA.MintX509SVID(t, "spiffe://client.test/other")
	ours := f.clientCA.MintX509SVID(t, clientID)
	// The Workload API's default SVID is the first one.
	f.publish(t, other, ours)

	presented, err := f.handshake(t, f.newSource(t).ClientConfig(), f.server)
	require.NoError(t, err)
	require.Equal(t, ours.Certificates[0].Raw, presented[0].Raw)
}

func TestDefaultSVIDWithoutAnID(t *testing.T) {
	f := newFixture(t)
	first := f.clientCA.MintX509SVID(t, "spiffe://client.test/first")
	f.publish(t, first, f.clientCA.MintX509SVID(t, clientID))

	source, err := New(t.Context(), Options{Enabled: true, Socket: f.api.Addr, ServerID: serverID})
	require.NoError(t, err)
	t.Cleanup(func() { _ = source.Close() })

	presented, err := f.handshake(t, source.ClientConfig(), f.server)
	require.NoError(t, err)
	require.Equal(t, first.Certificates[0].Raw, presented[0].Raw, "the Workload API's default SVID is its first")
}

func TestWaitsForTheSVIDToBeIssued(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, "spiffe://client.test/other"))

	ours := f.clientCA.MintX509SVID(t, clientID)
	go func() {
		time.Sleep(200 * time.Millisecond)
		f.publish(t, ours)
	}()

	presented, err := f.handshake(t, f.newSource(t).ClientConfig(), f.server)
	require.NoError(t, err)
	require.Equal(t, ours.Certificates[0].Raw, presented[0].Raw)
}

func TestStartupTimeout(t *testing.T) {
	previous := startupTimeout
	startupTimeout = 500 * time.Millisecond
	t.Cleanup(func() { startupTimeout = previous })

	t.Run("SVID never issued", func(t *testing.T) {
		f := newFixture(t)
		f.publish(t, f.clientCA.MintX509SVID(t, "spiffe://client.test/other"))

		_, err := New(t.Context(), f.options())
		require.ErrorContains(t, err, "did not issue an X.509-SVID for "+clientID)
	})

	t.Run("no Workload API", func(t *testing.T) {
		opts := Options{Enabled: true, Socket: "unix://" + t.TempDir() + "/absent.sock", ID: clientID, ServerID: serverID}

		_, err := New(t.Context(), opts)
		require.ErrorContains(t, err, "unable to receive X.509-SVIDs from the SPIFFE Workload API")
	})
}

func TestFailsClosedWhenTheSVIDIsWithdrawn(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, clientID))
	source := f.newSource(t)

	f.publish(t, f.clientCA.MintX509SVID(t, "spiffe://client.test/other"))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := f.handshake(t, source.ClientConfig(), f.server)
		assert.Error(c, err, "no other SVID may be presented in place of the configured one")
	}, 5*time.Second, 10*time.Millisecond)
}

func TestServerAuthentication(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, clientID))
	cfg := f.newSource(t).ClientConfig()

	t.Run("wrong SPIFFE ID", func(t *testing.T) {
		_, err := f.handshake(t, cfg, f.serverCA.MintX509SVID(t, "spiffe://server.test/postgres"))
		require.ErrorContains(t, err, "unexpected ID")
	})

	t.Run("trust domain without a bundle", func(t *testing.T) {
		impostor := spiffetest.NewCA(t, "server.test").MintX509SVID(t, serverID)
		_, err := f.handshake(t, cfg, impostor)
		require.ErrorContains(t, err, "could not verify leaf certificate")
	})

	t.Run("chain through an intermediate the server sends", func(t *testing.T) {
		// The bundle holds only the root, as a federated SPIRE bundle does;
		// the server sends its leaf and intermediate, as OpenSSL does when the
		// intermediate is in its ssl_ca.
		chained := f.serverCA.Intermediate(t).MintX509SVID(t, serverID, "mysql.server.test")
		require.Len(t, chained.Certificates, 2)

		_, err := f.handshake(t, cfg, chained)
		require.NoError(t, err)
	})

	t.Run("host name is not what is verified", func(t *testing.T) {
		withName := f.serverCA.MintX509SVID(t, serverID, "mysql.server.test")
		_, err := f.handshake(t, cfg, withName)
		require.NoError(t, err)
	})
}

func TestServerAuthorizationDefaults(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, clientID))

	newConfig := func(t *testing.T, serverID string) *tls.Config {
		t.Helper()
		source, err := New(t.Context(), Options{Enabled: true, Socket: f.api.Addr, ID: clientID, ServerID: serverID})
		require.NoError(t, err)
		t.Cleanup(func() { _ = source.Close() })
		return source.ClientConfig()
	}
	sameDomainServer := f.clientCA.MintX509SVID(t, "spiffe://client.test/mysql")

	t.Run("unset accepts the client's own trust domain", func(t *testing.T) {
		_, err := f.handshake(t, newConfig(t, ""), sameDomainServer)
		require.NoError(t, err)
	})

	t.Run("unset rejects a federated trust domain", func(t *testing.T) {
		_, err := f.handshake(t, newConfig(t, ""), f.server)
		require.ErrorContains(t, err, "unexpected trust domain")
	})

	t.Run("trust domain accepts any of its workloads", func(t *testing.T) {
		cfg := newConfig(t, "spiffe://server.test")
		for _, id := range []string{serverID, "spiffe://server.test/postgres"} {
			_, err := f.handshake(t, cfg, f.serverCA.MintX509SVID(t, id))
			require.NoError(t, err, id)
		}
	})

	t.Run("trust domain rejects another trust domain", func(t *testing.T) {
		_, err := f.handshake(t, newConfig(t, "spiffe://server.test"), sameDomainServer)
		require.ErrorContains(t, err, "unexpected trust domain")
	})
}

func TestExpiryMetric(t *testing.T) {
	f := newFixture(t)
	svid := f.clientCA.MintX509SVID(t, clientID)
	f.publish(t, svid)
	source := f.newSource(t)

	registry := prometheus.NewRegistry()
	for _, collector := range source.Collectors("primary") {
		require.NoError(t, registry.Register(collector))
	}

	require.InDelta(t, float64(svid.Certificates[0].NotAfter.Unix()),
		testutil.ToFloat64(source.Collectors("primary")[0]), 0)
}

func TestCloseEndsTheStream(t *testing.T) {
	f := newFixture(t)
	f.publish(t, f.clientCA.MintX509SVID(t, clientID))

	source, err := New(t.Context(), f.options())
	require.NoError(t, err)
	cfg := source.ClientConfig()
	require.NoError(t, source.Close())

	_, err = f.handshake(t, cfg, f.server)
	require.ErrorContains(t, err, "source is closed")
}
