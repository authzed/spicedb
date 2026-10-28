package x509util

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeKeyPair writes cert and its private key to certPath and keyPath as PEM.
func writeKeyPair(t *testing.T, certPath, keyPath string, cert tls.Certificate) {
	t.Helper()
	writeCert(t, certPath, cert)
	writeKey(t, keyPath, cert)
}

func writeCert(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600))
}

func writeKey(t *testing.T, path string, cert tls.Certificate) {
	t.Helper()
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600))
}

func keyPairPaths(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
}

func servedLeaf(t *testing.T, kp *KeyPair) []byte {
	t.Helper()
	cert, err := kp.GetCertificate(nil)
	require.NoError(t, err)
	require.NotNil(t, cert)
	return cert.Certificate[0]
}

func TestNewKeyPair(t *testing.T) {
	t.Run("errors on a missing certificate", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		_, err := NewKeyPair(certPath, keyPath)
		require.Error(t, err)
	})

	t.Run("errors on a key that does not match the certificate", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		authority := newTestAuthority(t, "ca")
		writeCert(t, certPath, authority.issueLeaf(t, "example.com"))
		writeKey(t, keyPath, authority.issueLeaf(t, "example.com"))

		_, err := NewKeyPair(certPath, keyPath)
		require.Error(t, err)
	})
}

func TestKeyPairGetCertificate(t *testing.T) {
	authority := newTestAuthority(t, "ca")

	t.Run("picks up a rotated key pair", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		oldLeaf := authority.issueLeaf(t, "example.com")
		writeKeyPair(t, certPath, keyPath, oldLeaf)

		kp, err := NewKeyPair(certPath, keyPath)
		require.NoError(t, err)
		kp.WithReloadInterval(time.Nanosecond)
		require.Equal(t, oldLeaf.Certificate[0], servedLeaf(t, kp))

		newLeaf := authority.issueLeaf(t, "example.com")
		writeKeyPair(t, certPath, keyPath, newLeaf)
		require.Equal(t, newLeaf.Certificate[0], servedLeaf(t, kp), "the rotated certificate should be served")
	})

	t.Run("does not re-read within the reload interval", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		oldLeaf := authority.issueLeaf(t, "example.com")
		writeKeyPair(t, certPath, keyPath, oldLeaf)

		kp, err := NewKeyPair(certPath, keyPath)
		require.NoError(t, err)
		kp.WithReloadInterval(time.Hour)

		writeKeyPair(t, certPath, keyPath, authority.issueLeaf(t, "example.com"))
		require.Equal(t, oldLeaf.Certificate[0], servedLeaf(t, kp))
	})

	t.Run("keeps the previous pair when the files go away", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		oldLeaf := authority.issueLeaf(t, "example.com")
		writeKeyPair(t, certPath, keyPath, oldLeaf)

		kp, err := NewKeyPair(certPath, keyPath)
		require.NoError(t, err)
		kp.WithReloadInterval(time.Nanosecond)

		require.NoError(t, os.Remove(keyPath))
		require.Equal(t, oldLeaf.Certificate[0], servedLeaf(t, kp))
	})

	t.Run("keeps the previous pair while only the certificate has been rotated", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		oldLeaf := authority.issueLeaf(t, "example.com")
		writeKeyPair(t, certPath, keyPath, oldLeaf)

		kp, err := NewKeyPair(certPath, keyPath)
		require.NoError(t, err)
		kp.WithReloadInterval(time.Nanosecond)

		newLeaf := authority.issueLeaf(t, "example.com")
		writeCert(t, certPath, newLeaf)
		require.Equal(t, oldLeaf.Certificate[0], servedLeaf(t, kp), "a certificate without its key should not be served")

		writeKey(t, keyPath, newLeaf)
		require.Equal(t, newLeaf.Certificate[0], servedLeaf(t, kp))
	})

	t.Run("returns the same certificate when the contents are unchanged", func(t *testing.T) {
		certPath, keyPath := keyPairPaths(t)
		writeKeyPair(t, certPath, keyPath, authority.issueLeaf(t, "example.com"))

		kp, err := NewKeyPair(certPath, keyPath)
		require.NoError(t, err)
		kp.WithReloadInterval(time.Nanosecond)

		first, err := kp.GetCertificate(nil)
		require.NoError(t, err)
		second, err := kp.GetCertificate(nil)
		require.NoError(t, err)
		require.Same(t, first, second)
	})
}

// TestKeyPairServesRotatedCertificate drives real TLS handshakes to show that a
// key pair rotated on disk is presented without rebuilding the listener.
func TestKeyPairServesRotatedCertificate(t *testing.T) {
	authority := newTestAuthority(t, "ca")
	roots := x509.NewCertPool()
	roots.AddCert(authority.cert)

	certPath, keyPath := keyPairPaths(t)
	oldLeaf := authority.issueLeaf(t, "example.com")
	writeKeyPair(t, certPath, keyPath, oldLeaf)
	kp, err := NewKeyPair(certPath, keyPath)
	require.NoError(t, err)
	kp.WithReloadInterval(time.Nanosecond)

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		GetCertificate: kp.GetCertificate,
		MinVersion:     tls.VersionTLS12,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.(*tls.Conn).HandshakeContext(t.Context())
			_ = conn.Close()
		}
	}()

	presented := func() []byte {
		dialer := &tls.Dialer{Config: &tls.Config{RootCAs: roots, ServerName: "example.com", MinVersion: tls.VersionTLS12}}
		conn, err := dialer.DialContext(t.Context(), "tcp", listener.Addr().String())
		require.NoError(t, err)
		defer func() { _ = conn.Close() }()
		return conn.(*tls.Conn).ConnectionState().PeerCertificates[0].Raw
	}

	require.Equal(t, oldLeaf.Certificate[0], presented())

	newLeaf := authority.issueLeaf(t, "example.com")
	writeKeyPair(t, certPath, keyPath, newLeaf)
	require.Equal(t, newLeaf.Certificate[0], presented(), "the rotated certificate should be presented")
}

// writeTLSSecretRevision publishes cert as a kubernetes.io/tls Secret revision
// under target the way kubelet's AtomicWriter does, swapping both files at once.
func writeTLSSecretRevision(t *testing.T, target, stamp string, cert tls.Certificate) string {
	t.Helper()

	revision := filepath.Join(target, stamp)
	require.NoError(t, os.MkdirAll(revision, 0o755))
	writeKeyPair(t, filepath.Join(revision, "tls.crt"), filepath.Join(revision, "tls.key"), cert)

	pending := filepath.Join(target, "..data_tmp")
	require.NoError(t, os.Symlink(stamp, pending))
	require.NoError(t, os.Rename(pending, filepath.Join(target, "..data")))

	return revision
}

// TestKeyPairFollowsKubernetesSecretRotation covers a serving key pair mounted
// from a Secret, where an update replaces both files' inodes at once.
func TestKeyPairFollowsKubernetesSecretRotation(t *testing.T) {
	authority := newTestAuthority(t, "ca")
	target := t.TempDir()
	oldLeaf := authority.issueLeaf(t, "example.com")
	retired := writeTLSSecretRevision(t, target, "..2026_01_01_00_00_00.000000001", oldLeaf)

	certPath, keyPath := filepath.Join(target, "tls.crt"), filepath.Join(target, "tls.key")
	require.NoError(t, os.Symlink(filepath.Join("..data", "tls.crt"), certPath))
	require.NoError(t, os.Symlink(filepath.Join("..data", "tls.key"), keyPath))

	kp, err := NewKeyPair(certPath, keyPath)
	require.NoError(t, err)
	kp.WithReloadInterval(time.Nanosecond)
	require.Equal(t, oldLeaf.Certificate[0], servedLeaf(t, kp))

	newLeaf := authority.issueLeaf(t, "example.com")
	writeTLSSecretRevision(t, target, "..2026_01_01_00_05_00.000000002", newLeaf)
	require.NoError(t, os.RemoveAll(retired))

	require.Equal(t, newLeaf.Certificate[0], servedLeaf(t, kp),
		"the key pair published by the new secret revision should be served")
}

func TestKeyPairGetCertificateConcurrently(t *testing.T) {
	authority := newTestAuthority(t, "ca")
	certPath, keyPath := keyPairPaths(t)
	writeKeyPair(t, certPath, keyPath, authority.issueLeaf(t, "example.com"))

	kp, err := NewKeyPair(certPath, keyPath)
	require.NoError(t, err)
	kp.WithReloadInterval(time.Nanosecond)

	done := make(chan struct{})
	errs := make(chan error, 8)
	for range 8 {
		go func() {
			for {
				select {
				case <-done:
					errs <- nil
					return
				default:
				}
				cert, err := kp.GetCertificate(nil)
				if err == nil && (cert == nil || cert.Leaf == nil) {
					err = errors.New("served a nil certificate")
				}
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	for range 20 {
		writeKeyPair(t, certPath, keyPath, authority.issueLeaf(t, "example.com"))
	}
	close(done)
	for range 8 {
		require.NoError(t, <-errs)
	}
}
