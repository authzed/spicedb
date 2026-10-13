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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// ca is a generated certificate authority that can sign serving certificates.
type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newCA(t *testing.T, org string) *ca {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{org}},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &ca{cert: cert, key: key}
}

// writeBundle writes the CA's certificate to path as a PEM bundle.
func (c *ca) writeBundle(t *testing.T, path string) {
	t.Helper()
	writePEM(t, path, "CERTIFICATE", c.cert.Raw)
}

// issueServingCert signs a certificate for 127.0.0.1 and writes it and its key.
func (c *ca) issueServingCert(t *testing.T, certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{"spicedb-test"}},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(1 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, c.cert, &key.PublicKey, c.key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	writePEM(t, certPath, "CERTIFICATE", der)
	writePEM(t, keyPath, "EC PRIVATE KEY", keyDER)
}

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	require.NotNil(t, buf)
	require.NoError(t, os.WriteFile(path, buf, 0o600))
}

// TestClientCredsFollowCARotation asserts that the credentials returned by
// clientCreds() keep working after the CA and serving certificate are rotated on
// disk, which they cannot do while the pool is captured in tls.Config.RootCAs.
func TestClientCredsFollowCARotation(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")

	oldCA := newCA(t, "oldCA")
	oldCA.writeBundle(t, caPath)
	oldCA.issueServingCert(t, certPath, keyPath)

	cfg := &GRPCServerConfig{
		Address:      "127.0.0.1:0",
		Network:      "tcp",
		Enabled:      true,
		MaxConnAge:   30 * time.Second,
		TLSCertPath:  certPath,
		TLSKeyPath:   keyPath,
		ClientCAPath: caPath,
	}

	srv, err := cfg.Complete(zerolog.Disabled, func(s *grpc.Server) {
		healthpb.RegisterHealthServer(s, health.NewServer())
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.Run(ctx) }()

	// Complete resolved the :0 port on the real listener.
	addr := srv.(*completedGRPCServer).listener.Addr().String()

	creds, err := cfg.clientCreds()
	require.NoError(t, err)

	// A second set pinned to the retiring CA and never reloaded.
	retiredPool := x509.NewCertPool()
	retiredPool.AddCert(oldCA.cert)
	retiredCreds := credentials.NewTLS(&tls.Config{RootCAs: retiredPool, MinVersion: tls.VersionTLS12})

	require.NoError(t, probe(t, addr, creds), "handshake should succeed before rotation")
	require.NoError(t, probe(t, addr, retiredCreds), "the retiring CA should be trusted before rotation")

	newCAuth := newCA(t, "newCA")
	newCAuth.issueServingCert(t, certPath, keyPath)
	newCAuth.writeBundle(t, caPath)

	// Waiting for the pinned credentials to start failing first means the
	// assertion below cannot pass just because nothing has rotated yet.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.Error(collect, probe(t, addr, retiredCreds))
	}, 30*time.Second, 250*time.Millisecond, "server kept serving a certificate signed by the retired CA")

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, probe(t, addr, creds))
	}, 30*time.Second, 250*time.Millisecond, "credentials never picked up the rotated CA")
}

// probe dials addr with the given credentials and issues one health check,
// returning the error the RPC failed with, or nil.
func probe(t *testing.T, addr string, creds credentials.TransportCredentials) error {
	t.Helper()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil && status.Code(err) == codes.NotFound {
		// TLS succeeded; the health server just has no entry for "".
		return nil
	}
	return err
}
