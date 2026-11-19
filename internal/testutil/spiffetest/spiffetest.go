// Package spiffetest provides a fake SPIFFE Workload API and the certificate
// authorities to populate it, for testing clients that obtain X.509-SVIDs and
// trust bundles from the Workload API.
package spiffetest

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/proto/spiffe/workload"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CA is a certificate authority for one trust domain. A CA made with
// Intermediate signs on behalf of its root, so the SVIDs it mints carry the
// intermediate in their chain while the bundle holds only the root.
type CA struct {
	TrustDomain spiffeid.TrustDomain

	cert   *x509.Certificate
	key    crypto.Signer
	chain  []*x509.Certificate // intermediates between cert and the root, cert first
	bundle []*x509.Certificate
}

// NewCA returns a self-signed root CA for trustDomain.
func NewCA(t testing.TB, trustDomain string) *CA {
	t.Helper()

	td, err := spiffeid.TrustDomainFromString(trustDomain)
	require.NoError(t, err)

	key := newKey(t)
	template := &x509.Certificate{
		SerialNumber:          newSerial(t),
		Subject:               pkix.Name{Organization: []string{"SPIFFE"}, CommonName: td.String() + " root"},
		URIs:                  []*url.URL{td.ID().URL()},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	cert := createCertificate(t, template, template, key.Public(), key)

	return &CA{TrustDomain: td, cert: cert, key: key, bundle: []*x509.Certificate{cert}}
}

// Intermediate returns a CA for the same trust domain, signed by ca.
func (ca *CA) Intermediate(t testing.TB) *CA {
	t.Helper()

	key := newKey(t)
	template := &x509.Certificate{
		SerialNumber:          newSerial(t),
		Subject:               pkix.Name{Organization: []string{"SPIFFE"}, CommonName: ca.TrustDomain.String() + " intermediate"},
		URIs:                  []*url.URL{ca.TrustDomain.ID().URL()},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	cert := createCertificate(t, template, ca.cert, key.Public(), ca.key)

	return &CA{
		TrustDomain: ca.TrustDomain,
		cert:        cert,
		key:         key,
		chain:       append([]*x509.Certificate{cert}, ca.chain...),
		bundle:      ca.bundle,
	}
}

// Bundle returns the root certificates of the trust domain.
func (ca *CA) Bundle() []*x509.Certificate {
	return ca.bundle
}

// MintX509SVID issues an X.509-SVID for id, which must be in the CA's trust
// domain. When dnsNames are given, the first one becomes the subject common
// name, as SPIRE does; databases that authenticate clients by subject rely on
// it.
func (ca *CA) MintX509SVID(t testing.TB, id string, dnsNames ...string) *x509svid.SVID {
	t.Helper()

	spiffeID, err := spiffeid.FromString(id)
	require.NoError(t, err)
	require.Equal(t, ca.TrustDomain, spiffeID.TrustDomain(), "the SVID must be in the CA's trust domain")

	subject := pkix.Name{Country: []string{"US"}, Organization: []string{"SPIRE"}}
	if len(dnsNames) > 0 {
		subject.CommonName = dnsNames[0]
	}

	key := newKey(t)
	template := &x509.Certificate{
		SerialNumber:          newSerial(t),
		Subject:               subject,
		URIs:                  []*url.URL{spiffeID.URL()},
		DNSNames:              dnsNames,
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageKeyAgreement,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	leaf := createCertificate(t, template, ca.cert, key.Public(), ca.key)

	return &x509svid.SVID{
		ID:           spiffeID,
		Certificates: append([]*x509.Certificate{leaf}, ca.chain...),
		PrivateKey:   key,
	}
}

// TLSCertificate returns svid as a certificate for a TLS server or client.
func TLSCertificate(svid *x509svid.SVID) tls.Certificate {
	raw := make([][]byte, 0, len(svid.Certificates))
	for _, cert := range svid.Certificates {
		raw = append(raw, cert.Raw)
	}
	return tls.Certificate{Certificate: raw, PrivateKey: svid.PrivateKey, Leaf: svid.Certificates[0]}
}

// WorkloadAPI is a fake SPIFFE Workload API serving X.509-SVIDs over a unix
// socket. Until SetX509SVIDs is called it denies every request, as SPIRE does
// for a workload that has no registration entry.
type WorkloadAPI struct {
	workload.UnimplementedSpiffeWorkloadAPIServer

	// Addr is the address clients dial, in the unix:// form.
	Addr string

	mu       sync.Mutex
	response *workload.X509SVIDResponse // GUARDED_BY(mu)
	updated  chan struct{}              // GUARDED_BY(mu), closed and replaced on every update
}

// NewWorkloadAPI starts a fake Workload API that is stopped when the test ends.
func NewWorkloadAPI(t testing.TB) *WorkloadAPI {
	t.Helper()

	socket := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)

	api := &WorkloadAPI{Addr: "unix://" + socket, updated: make(chan struct{})}

	server := grpc.NewServer()
	workload.RegisterSpiffeWorkloadAPIServer(server, api)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	return api
}

// SetX509SVIDs publishes svids to every client, as the Workload API does when
// an SVID or bundle rotates. The bundle of each SVID's trust domain and any
// federated trust domain is taken from cas.
func (w *WorkloadAPI) SetX509SVIDs(t testing.TB, svids []*x509svid.SVID, cas ...*CA) {
	t.Helper()

	bundles := make(map[spiffeid.TrustDomain][]byte, len(cas))
	for _, ca := range cas {
		bundles[ca.TrustDomain] = concatDER(ca.Bundle())
	}

	response := &workload.X509SVIDResponse{FederatedBundles: map[string][]byte{}}
	own := map[spiffeid.TrustDomain]struct{}{}
	for _, svid := range svids {
		bundle, ok := bundles[svid.ID.TrustDomain()]
		require.True(t, ok, "no CA given for the trust domain of %s", svid.ID)

		key, err := x509.MarshalPKCS8PrivateKey(svid.PrivateKey)
		require.NoError(t, err)

		response.Svids = append(response.Svids, &workload.X509SVID{
			SpiffeId:    svid.ID.String(),
			X509Svid:    concatDER(svid.Certificates),
			X509SvidKey: key,
			Bundle:      bundle,
		})
		own[svid.ID.TrustDomain()] = struct{}{}
	}
	for td, bundle := range bundles {
		if _, ok := own[td]; !ok {
			response.FederatedBundles[td.IDString()] = bundle
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.response = response
	close(w.updated)
	w.updated = make(chan struct{})
}

func (w *WorkloadAPI) current() (*workload.X509SVIDResponse, <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.response, w.updated
}

// FetchX509SVID streams the current SVIDs, and again on every update.
func (w *WorkloadAPI) FetchX509SVID(_ *workload.X509SVIDRequest, stream grpc.ServerStreamingServer[workload.X509SVIDResponse]) error {
	for {
		response, updated := w.current()
		if response == nil {
			return status.Error(codes.PermissionDenied, "no identity issued")
		}
		if err := stream.Send(response); err != nil {
			return err
		}

		select {
		case <-updated:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func concatDER(certs []*x509.Certificate) []byte {
	var der []byte
	for _, cert := range certs {
		der = append(der, cert.Raw...)
	}
	return der
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

func newSerial(t testing.TB) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	return serial
}

func createCertificate(t testing.TB, template, parent *x509.Certificate, pub crypto.PublicKey, signer crypto.Signer) *x509.Certificate {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, template, parent, pub, signer)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return cert
}
