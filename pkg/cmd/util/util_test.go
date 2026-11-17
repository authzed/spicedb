package util

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestDisabledGRPC(t *testing.T) {
	s, err := (&GRPCServerConfig{Enabled: false}).Complete(zerolog.InfoLevel, nil)
	require.NoError(t, err)
	require.NoError(t, s.Run(t.Context()))
	require.True(t, s.Insecure())
	s.GracefulStop()
}

func TestDisabledHTTP(t *testing.T) {
	s, err := (&HTTPServerConfig{HTTPEnabled: false}).Complete(zerolog.InfoLevel, nil)
	require.NoError(t, err)
	require.NoError(t, s.ListenAndServe())
	s.Close()
}

func TestValidateTLSConfig(t *testing.T) {
	t.Run("both options present is fine", func(t *testing.T) {
		require.NoError(t, (&GRPCServerConfig{TLSCertPath: "some path", TLSKeyPath: "some other path"}).validateTLSConfig())
	})
	t.Run("neither option present is fine", func(t *testing.T) {
		require.NoError(t, (&GRPCServerConfig{}).validateTLSConfig())
	})
	t.Run("one present but not the other returns an error", func(t *testing.T) {
		require.ErrorContains(t, (&GRPCServerConfig{TLSCertPath: "some path"}).validateTLSConfig(), "must provide both")
	})
}

// watchHealth opens a health Watch, a stream that never ends, on a buffered server with these limits,
// and returns a channel that gets the stream's error once the server ends it.
func watchHealth(t *testing.T, age, grace time.Duration) <-chan error {
	t.Helper()
	c := &GRPCServerConfig{Enabled: true, Network: BufferedNetwork, MaxConnAge: age, MaxConnAgeGrace: grace}
	s, err := c.Complete(zerolog.InfoLevel, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, health.NewServer()) })
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() { _ = s.Run(ctx) }()
	t.Cleanup(s.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///buffnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return s.BufferedListener().DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		done <- err
	}()
	return done
}

func TestMaxConnAgeGraceEndsLongStream(t *testing.T) {
	t.Parallel()
	select {
	case err := <-watchHealth(t, 100*time.Millisecond, 200*time.Millisecond):
		require.Equal(t, codes.Unavailable, status.Code(err), "%v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("the stream outlived max age + grace")
	}
}

func TestMaxConnAgeWithoutGraceKeepsLongStream(t *testing.T) {
	t.Parallel()
	select {
	case err := <-watchHealth(t, 100*time.Millisecond, 0):
		t.Fatalf("the stream ended: %v", err)
	case <-time.After(time.Second):
	}
}

func TestMaxConnAgeGraceFlag(t *testing.T) {
	t.Parallel()
	c := &GRPCServerConfig{}
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	RegisterGRPCServerFlags(fs, c, "grpc", "grpc", ":50051", true)
	require.NoError(t, fs.Parse(nil))
	require.Zero(t, c.MaxConnAgeGrace, "no grace by default: grpc-go waits forever")
	require.NoError(t, fs.Parse([]string{"--grpc-max-conn-age-grace=1h"}))
	require.Equal(t, time.Hour, c.MaxConnAgeGrace)
}

func TestTLSOpts(t *testing.T) {
	t.Run("no TLS configuration returns no options", func(t *testing.T) {
		opts, err := (&GRPCServerConfig{}).tlsOpts()
		require.NoError(t, err)
		require.Empty(t, opts)
	})
	t.Run("a certificate without a key returns an error", func(t *testing.T) {
		_, err := (&GRPCServerConfig{TLSCertPath: "some path"}).tlsOpts()
		require.ErrorContains(t, err, "must provide both")
	})
	t.Run("a missing key pair returns an error", func(t *testing.T) {
		dir := t.TempDir()
		_, err := (&GRPCServerConfig{TLSCertPath: filepath.Join(dir, "tls.crt"), TLSKeyPath: filepath.Join(dir, "tls.key")}).tlsOpts()
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

func TestHTTPServerMissingKeyPair(t *testing.T) {
	dir := t.TempDir()
	_, err := (&HTTPServerConfig{
		HTTPAddress:     "127.0.0.1:0",
		HTTPTLSCertPath: filepath.Join(dir, "tls.crt"),
		HTTPTLSKeyPath:  filepath.Join(dir, "tls.key"),
		HTTPEnabled:     true,
	}).Complete(zerolog.InfoLevel, http.NotFoundHandler())
	require.ErrorIs(t, err, os.ErrNotExist)
}

// writeSelfSignedKeyPair writes a self-signed certificate for commonName and
// its key to certPath and keyPath.
func writeSelfSignedKeyPair(t *testing.T, certPath, keyPath, commonName string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
}

func TestHTTPServerPicksUpRotatedKeyPair(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	writeSelfSignedKeyPair(t, certPath, keyPath, "original")

	// Reserve a free port, since the server only takes an address.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())

	s, err := (&HTTPServerConfig{
		HTTPAddress:           addr,
		HTTPTLSCertPath:       certPath,
		HTTPTLSKeyPath:        keyPath,
		HTTPEnabled:           true,
		HTTPTLSReloadInterval: time.Millisecond,
	}).Complete(zerolog.InfoLevel, http.NotFoundHandler())
	require.NoError(t, err)
	go func() { _ = s.ListenAndServe() }()
	t.Cleanup(s.Close)

	presented := func() string {
		dialer := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}} //nolint:gosec // the test only reads the presented certificate
		conn, err := dialer.DialContext(t.Context(), "tcp", addr)
		if err != nil {
			return ""
		}
		defer func() { _ = conn.Close() }()
		return conn.(*tls.Conn).ConnectionState().PeerCertificates[0].Subject.CommonName
	}

	require.Eventually(t, func() bool { return presented() == "original" }, 5*time.Second, 10*time.Millisecond)

	writeSelfSignedKeyPair(t, certPath, keyPath, "rotated")
	require.Eventually(t, func() bool { return presented() == "rotated" }, 5*time.Second, 10*time.Millisecond,
		"the HTTP server should present the rotated certificate")
}
