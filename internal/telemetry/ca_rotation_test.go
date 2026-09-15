package telemetry

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestRemoteReporterFollowsCARotation asserts that the reporter keeps reaching
// its endpoint after the CA override is rotated on disk, which it cannot do
// while the pool is captured in the transport's TLSClientConfig.
func TestRemoteReporterFollowsCARotation(t *testing.T) {
	caPath := filepath.Join(t.TempDir(), "ca.crt")

	authority := newTestCA(t, "old")
	writeCABundle(t, caPath, authority)

	sink := newTLSSink(t, authority.issueServingCert(t))

	registry := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_metric", Help: "A test metric"})
	gauge.Set(42)
	registry.MustRegister(gauge)

	// The endpoint is on loopback, so sub-minute intervals are allowed and the
	// reporter pushes without a startup delay.
	reporter, err := RemoteReporter(registry, sink.url, caPath, time.Second)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- reporter(t.Context()) }()
	t.Cleanup(func() { <-done })

	require.Eventually(t, func() bool { return sink.reports() > 0 }, 15*time.Second, 100*time.Millisecond,
		"the reporter should reach the endpoint with the authority it started with")

	// Serve a certificate from a new authority without publishing it. The
	// handshake has to start failing here, otherwise the recovery below would
	// pass whether or not the trust bundle is ever re-read.
	rotated := newTestCA(t, "new")
	sink.setCert(rotated.issueServingCert(t))

	require.Eventually(t, func() bool { return strings.Contains(sink.handshakeErrors(), "TLS handshake error") },
		15*time.Second, 100*time.Millisecond,
		"the reporter should reject a certificate from an authority that is not in the bundle")

	writeCABundle(t, caPath, rotated)
	before := sink.reports()

	require.Eventually(t, func() bool { return sink.reports() > before }, 45*time.Second, 100*time.Millisecond,
		"the reporter should reach the endpoint again once the rotated authority is published")
}

// tlsSink is an HTTPS endpoint that counts the reports it receives and whose
// serving certificate can be swapped while it is running. It is also the
// server's error log, so the test can observe rejected handshakes.
type tlsSink struct {
	url string

	mu         sync.Mutex
	cert       tls.Certificate // GUARDED_BY(mu)
	received   int             // GUARDED_BY(mu)
	serverLogs strings.Builder // GUARDED_BY(mu)
}

func newTLSSink(t *testing.T, cert tls.Certificate) *tlsSink {
	t.Helper()

	sink := &tlsSink{cert: cert}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			sink.mu.Lock()
			sink.received++
			sink.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}),
		TLSConfig:         &tls.Config{GetCertificate: sink.getCertificate, MinVersion: tls.VersionTLS12},
		ErrorLog:          log.New(sink, "", 0),
		ReadHeaderTimeout: 5 * time.Second,
	}
	// Without this the reporter reuses a pooled connection and never re-dials,
	// so a rotated certificate would not be presented to it.
	server.SetKeepAlivesEnabled(false)

	go func() { _ = server.ServeTLS(listener, "", "") }()
	t.Cleanup(func() { _ = server.Close() })

	sink.url = "https://" + listener.Addr().String()
	return sink
}

func (s *tlsSink) getCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &s.cert, nil
}

func (s *tlsSink) setCert(cert tls.Certificate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cert = cert
}

func (s *tlsSink) reports() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.received
}

func (s *tlsSink) handshakeErrors() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serverLogs.String()
}

func (s *tlsSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serverLogs.Write(p)
}

// testCA is a generated certificate authority that can sign serving certificates.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func newTestCA(t *testing.T, org string) *testCA {
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

	return &testCA{cert: cert, key: key}
}

// issueServingCert signs a certificate for the loopback address the sink listens on.
func (c *testCA) issueServingCert(t *testing.T) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{"telemetry-sink"}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, c.cert, &key.PublicKey, c.key)
	require.NoError(t, err)

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func writeCABundle(t *testing.T, path string, authority *testCA) {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: authority.cert.Raw})
	require.NotNil(t, buf)
	require.NoError(t, os.WriteFile(path, buf, 0o600))
}
