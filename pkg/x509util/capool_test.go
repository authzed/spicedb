package x509util

import (
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

	"github.com/stretchr/testify/require"
)

// testAuthority is a certificate authority that can sign leaf certificates.
type testAuthority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestAuthority(t *testing.T, org string) *testAuthority {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{org}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return &testAuthority{cert: cert, key: key}
}

// writeBundle writes the authority's certificate to path as a PEM bundle.
func (a *testAuthority) writeBundle(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, a.bundlePEM(t), 0o600))
}

func (a *testAuthority) bundlePEM(t *testing.T) []byte {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.cert.Raw})
	require.NotNil(t, buf)
	return buf
}

// issueLeaf signs a serving certificate for dnsName.
func (a *testAuthority) issueLeaf(t *testing.T, dnsName string) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{"leaf"}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{dnsName},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// verifiesWith reports whether cert chains up to any authority in pool.
func verifiesWith(cert tls.Certificate, pool *x509.CertPool) bool {
	_, err := cert.Leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return err == nil
}

func TestNewCAPool(t *testing.T) {
	t.Run("errors on a missing path", func(t *testing.T) {
		_, err := NewCAPool(filepath.Join(t.TempDir(), "absent.crt"))
		require.Error(t, err)
	})

	t.Run("errors on a bundle that holds no certificates", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "garbage.crt")
		require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0o600))

		_, err := NewCAPool(path)
		require.ErrorContains(t, err, "failed to append certs from CA PEM")
	})

	t.Run("reads a directory of bundles", func(t *testing.T) {
		dir := t.TempDir()
		first := newTestAuthority(t, "first")
		second := newTestAuthority(t, "second")
		first.writeBundle(t, filepath.Join(dir, "first.crt"))
		second.writeBundle(t, filepath.Join(dir, "second.crt"))

		pool, err := NewCAPool(dir)
		require.NoError(t, err)

		require.True(t, verifiesWith(first.issueLeaf(t, "example.com"), pool.CertPool()))
		require.True(t, verifiesWith(second.issueLeaf(t, "example.com"), pool.CertPool()))
	})
}

func TestCAPoolCertPool(t *testing.T) {
	t.Run("picks up a rotated authority", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.crt")
		oldAuthority := newTestAuthority(t, "old")
		oldAuthority.writeBundle(t, path)

		pool, err := NewCAPool(path)
		require.NoError(t, err)
		pool.WithReloadInterval(time.Nanosecond)

		oldLeaf := oldAuthority.issueLeaf(t, "example.com")
		require.True(t, verifiesWith(oldLeaf, pool.CertPool()))

		newAuthority := newTestAuthority(t, "new")
		newAuthority.writeBundle(t, path)
		newLeaf := newAuthority.issueLeaf(t, "example.com")

		require.True(t, verifiesWith(newLeaf, pool.CertPool()), "the rotated authority should be trusted")
		require.False(t, verifiesWith(oldLeaf, pool.CertPool()), "the retired authority should no longer be trusted")
	})

	t.Run("does not re-read within the reload interval", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.crt")
		oldAuthority := newTestAuthority(t, "old")
		oldAuthority.writeBundle(t, path)

		pool, err := NewCAPool(path)
		require.NoError(t, err)
		pool.WithReloadInterval(time.Hour)

		newAuthority := newTestAuthority(t, "new")
		newAuthority.writeBundle(t, path)

		require.False(t, verifiesWith(newAuthority.issueLeaf(t, "example.com"), pool.CertPool()))
		require.True(t, verifiesWith(oldAuthority.issueLeaf(t, "example.com"), pool.CertPool()))
	})

	t.Run("keeps the previous authorities when the path goes away", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.crt")
		authority := newTestAuthority(t, "only")
		authority.writeBundle(t, path)

		pool, err := NewCAPool(path)
		require.NoError(t, err)
		pool.WithReloadInterval(time.Nanosecond)

		require.NoError(t, os.Remove(path))

		require.True(t, verifiesWith(authority.issueLeaf(t, "example.com"), pool.CertPool()),
			"a disappearing bundle should not empty the trust store")
	})

	t.Run("keeps the previous authorities when the bundle is unreadable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.crt")
		authority := newTestAuthority(t, "only")
		authority.writeBundle(t, path)

		pool, err := NewCAPool(path)
		require.NoError(t, err)
		pool.WithReloadInterval(time.Nanosecond)

		// A rotation caught partway through writing looks like this.
		require.NoError(t, os.WriteFile(path, []byte("-----BEGIN CERTIFICATE-----\ntrunc"), 0o600))

		require.True(t, verifiesWith(authority.issueLeaf(t, "example.com"), pool.CertPool()),
			"a truncated bundle should not empty the trust store")
	})

	t.Run("returns the same pool when the contents are unchanged", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "ca.crt")
		authority := newTestAuthority(t, "only")
		authority.writeBundle(t, path)

		pool, err := NewCAPool(path)
		require.NoError(t, err)
		pool.WithReloadInterval(time.Nanosecond)

		first := pool.CertPool()
		// Rewrite byte-identical contents, as a no-op reconcile loop would.
		authority.writeBundle(t, path)

		require.Same(t, first, pool.CertPool())
	})
}

// writeSecretRevision lays out target the way kubelet's AtomicWriter does: the
// bundle lives in a timestamped directory that ..data points at, and the visible
// ca.crt is a symlink through ..data. Publishing a revision writes a new
// directory and renames ..data over it, so the visible path keeps resolving but
// its inode is replaced.
//
// Algorithm from k8s.io/kubernetes/pkg/volume/util/atomic_writer.go.
func writeSecretRevision(t *testing.T, target, stamp string, bundle []byte) string {
	t.Helper()

	revision := filepath.Join(target, stamp)
	require.NoError(t, os.MkdirAll(revision, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(revision, "ca.crt"), bundle, 0o600))

	pending := filepath.Join(target, "..data_tmp")
	require.NoError(t, os.Symlink(stamp, pending))
	require.NoError(t, os.Rename(pending, filepath.Join(target, "..data")))

	return revision
}

// TestCAPoolFollowsKubernetesSecretRotation covers the deployment shape this fix
// exists for: a CA bundle mounted from a Secret, where an update replaces the
// file's inode rather than writing through the path.
func TestCAPoolFollowsKubernetesSecretRotation(t *testing.T) {
	target := t.TempDir()
	oldAuthority := newTestAuthority(t, "old")
	retired := writeSecretRevision(t, target, "..2026_01_01_00_00_00.000000001", oldAuthority.bundlePEM(t))

	caPath := filepath.Join(target, "ca.crt")
	require.NoError(t, os.Symlink(filepath.Join("..data", "ca.crt"), caPath))

	pool, err := NewCAPool(caPath)
	require.NoError(t, err)
	pool.WithReloadInterval(time.Nanosecond)

	require.True(t, verifiesWith(oldAuthority.issueLeaf(t, "example.com"), pool.CertPool()))

	newAuthority := newTestAuthority(t, "new")
	writeSecretRevision(t, target, "..2026_01_01_00_05_00.000000002", newAuthority.bundlePEM(t))
	require.NoError(t, os.RemoveAll(retired))

	require.True(t, verifiesWith(newAuthority.issueLeaf(t, "example.com"), pool.CertPool()),
		"the authority published by the new secret revision should be trusted")
	require.False(t, verifiesWith(oldAuthority.issueLeaf(t, "example.com"), pool.CertPool()),
		"the authority from the retired revision should no longer be trusted")
}

// serveOnce listens on loopback and completes a single server-side handshake
// presenting cert, returning the address to dial.
func serveOnce(t *testing.T, cert tls.Certificate) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_ = tls.Server(conn, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2"},
		}).HandshakeContext(t.Context())
	}()

	return listener.Addr().String()
}

// TestReloadingCredsPicksUpRotatedCA drives a real TLS handshake to show that a
// CA rotated on disk takes effect without rebuilding the credentials.
func TestReloadingCredsPicksUpRotatedCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	oldAuthority := newTestAuthority(t, "old")
	oldAuthority.writeBundle(t, path)

	caPool, err := NewCAPool(path)
	require.NoError(t, err)
	caPool.WithReloadInterval(time.Nanosecond)

	creds := NewReloadingTLSCreds(caPool, &tls.Config{MinVersion: tls.VersionTLS12})

	handshake := func(serving tls.Certificate) error {
		client, err := net.Dial("tcp", serveOnce(t, serving))
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		// The authority carries the name that gets verified, so it has to match
		// the leaf rather than the loopback address dialed above.
		_, _, err = creds.ClientHandshake(t.Context(), "example.com:443", client)
		return err
	}

	require.NoError(t, handshake(oldAuthority.issueLeaf(t, "example.com")))

	newAuthority := newTestAuthority(t, "new")
	newLeaf := newAuthority.issueLeaf(t, "example.com")

	// Before the bundle is rotated the new authority is unknown.
	require.ErrorContains(t, handshake(newLeaf), "unknown authority")

	newAuthority.writeBundle(t, path)

	// The very same credentials now trust it.
	require.NoError(t, handshake(newLeaf))
}

// TestDialTLSContextPicksUpRotatedCA covers the http.Transport hook, which
// resolves the trust store once per connection rather than once per client.
func TestDialTLSContextPicksUpRotatedCA(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	oldAuthority := newTestAuthority(t, "old")
	oldAuthority.writeBundle(t, path)

	caPool, err := NewCAPool(path)
	require.NoError(t, err)
	caPool.WithReloadInterval(time.Nanosecond)

	// ServerName is set because the leaves are issued for a name, not for the
	// loopback address the dial function is handed.
	dial := caPool.DialTLSContext(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: "example.com"})

	connect := func(serving tls.Certificate) error {
		conn, err := dial(t.Context(), "tcp", serveOnce(t, serving))
		if err != nil {
			return err
		}
		return conn.Close()
	}

	require.NoError(t, connect(oldAuthority.issueLeaf(t, "example.com")))

	newAuthority := newTestAuthority(t, "new")
	newLeaf := newAuthority.issueLeaf(t, "example.com")
	require.ErrorContains(t, connect(newLeaf), "unknown authority")

	newAuthority.writeBundle(t, path)

	require.NoError(t, connect(newLeaf))
}

func TestReloadingCredsInfoAndClone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.crt")
	newTestAuthority(t, "only").writeBundle(t, path)

	caPool, err := NewCAPool(path)
	require.NoError(t, err)

	creds := NewReloadingTLSCreds(caPool, &tls.Config{MinVersion: tls.VersionTLS12})
	require.Equal(t, "tls", creds.Info().SecurityProtocol)

	//nolint:staticcheck // deprecated, but still part of the TransportCredentials interface
	require.NoError(t, creds.OverrideServerName("spicedb.example.com"))
	require.Equal(t, "spicedb.example.com", creds.(*reloadingCreds).base.ServerName)

	clone := creds.Clone()
	require.Equal(t, creds.Info(), clone.Info())
	// The clone shares the pool, so both reload on one schedule.
	require.Same(t, caPool, clone.(*reloadingCreds).ca)
}
