//go:build integration

package certtest_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/middleware/consistency"
	"github.com/authzed/spicedb/pkg/testutil"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

// servedCertRecorder records the leaf certificate presented by the server
// during each TLS handshake. It is safe for concurrent use.
type servedCertRecorder struct {
	mu   sync.Mutex
	last *x509.Certificate
}

func (r *servedCertRecorder) verifyConnection(cs tls.ConnectionState) error {
	if len(cs.PeerCertificates) == 0 {
		return fmt.Errorf("server presented no certificates")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last = cs.PeerCertificates[0]
	return nil
}

func (r *servedCertRecorder) lastServedCert() *x509.Certificate {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

func TestCertRotation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	certDir := t.TempDir()

	ca := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(5 * time.Minute),
		SerialNumber:          big.NewInt(0),
		Subject:               pkix.Name{Organization: []string{"testCA"}},
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caPublicKey := &caPrivateKey.PublicKey
	caCertBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublicKey, caPrivateKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caCertBytes)
	require.NoError(t, err)
	caFile, err := os.Create(filepath.Join(certDir, "ca.crt"))
	require.NoError(t, err)
	require.NoError(t, pem.Encode(caFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Raw,
	}))
	require.NoError(t, caFile.Close())

	old := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"initialTestCert"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(5 * time.Minute),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{"buffnet"},
	}
	oldCertPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	oldCertPublicKey := &oldCertPrivateKey.PublicKey
	oldCertBytes, err := x509.CreateCertificate(rand.Reader, old, caCert, oldCertPublicKey, caPrivateKey)
	require.NoError(t, err)
	oldCert, err := x509.ParseCertificate(oldCertBytes)
	require.NoError(t, err)

	keyFile, err := os.Create(filepath.Join(certDir, "tls.key"))
	require.NoError(t, err)
	oldKeyBytes, err := x509.MarshalECPrivateKey(oldCertPrivateKey)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyFile, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: oldKeyBytes,
	}))
	require.NoError(t, keyFile.Close())

	certFile, err := os.Create(filepath.Join(certDir, "tls.crt"))
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: oldCert.Raw,
	}))
	require.NoError(t, certFile.Close())

	// start a server with an initial set of certs
	emptyDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 10, time.Duration(90_000_000_000_000))
	require.NoError(t, err)
	ds, revision := tf.StandardDatastoreWithData(t, emptyDS)

	dispatcher, err := graph.NewLocalOnlyDispatcher(graph.MustNewDefaultDispatcherParametersForTesting())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cfg := server.NewConfigWithOptionsAndDefaults(
		server.WithDatastore(ds),
		server.WithDispatcher(dispatcher),
		server.WithDispatchMaxDepth(50),
		server.WithMaximumPreconditionCount(1000),
		server.WithMaximumUpdatesPerWrite(1000),
		server.WithGRPCServer(util.GRPCServerConfig{
			Network:     util.BufferedNetwork,
			Enabled:     true,
			TLSCertPath: certFile.Name(),
			TLSKeyPath:  keyFile.Name(),
			// ClientCAPath is used by the server's outbound dispatch credentials, not
			// for server-side client-cert validation — the server does not set ClientAuth,
			// so this test does not exercise mutual TLS.
			ClientCAPath: caFile.Name(),
		}),
		server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
		server.WithHTTPGateway(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{HTTPEnabled: false}),
		server.WithDispatchServer(util.GRPCServerConfig{Enabled: false}),
		server.SetUnaryMiddlewareModification([]server.MiddlewareModification[grpc.UnaryServerInterceptor]{
			{
				Operation: server.OperationReplaceAllUnsafe,
				Middlewares: []server.ReferenceableMiddleware[grpc.UnaryServerInterceptor]{
					{
						Name:       "datastore",
						Middleware: datalayer.UnaryServerInterceptor(datalayer.NewDataLayer(ds)),
					},
					{
						Name:       "consistency",
						Middleware: consistency.UnaryServerInterceptor("testing", consistency.TreatMismatchingTokensAsError),
					},
					{
						Name:       "servicespecific",
						Middleware: servicespecific.UnaryServerInterceptor,
					},
				},
			},
		}),
		server.SetStreamingMiddlewareModification([]server.MiddlewareModification[grpc.StreamServerInterceptor]{
			{
				Operation: server.OperationReplaceAllUnsafe,
				Middlewares: []server.ReferenceableMiddleware[grpc.StreamServerInterceptor]{
					{
						Name:       "datastore",
						Middleware: datalayer.StreamServerInterceptor(datalayer.NewDataLayer(ds)),
					},
					{
						Name:       "consistency",
						Middleware: consistency.StreamServerInterceptor("testing", consistency.TreatMismatchingTokensAsError),
					},
					{
						Name:       "servicespecific",
						Middleware: servicespecific.StreamServerInterceptor,
					},
				},
			},
		}),
	)
	// Disable caches and their metrics to avoid "duplicate metrics" errors
	cfg.DispatchClusterMetricsEnabled = false
	cfg.DispatchClientMetricsEnabled = false
	cfg.DatastoreConfig.EnableDatastoreMetrics = false
	cfg.NamespaceCacheConfig = server.CacheConfig{}
	cfg.DispatchCacheConfig = server.CacheConfig{}
	cfg.ClusterDispatchCacheConfig = server.CacheConfig{}
	cfg.LR3ResourceChunkCacheConfig = server.CacheConfig{}
	cfg.StoredSchemaCacheConfig = server.CacheConfig{}
	srv, err := cfg.Complete(ctx)
	require.NoError(t, err)

	wait := make(chan error, 1)
	go func() {
		err := srv.Run(ctx)
		wait <- err
	}()

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	// newConn dials a brand-new *grpc.ClientConn with its own servedCertRecorder.
	// A new ClientConn has no cached TLS session state (nil ClientSessionCache),
	// so every dial performs a full handshake and VerifyConnection fires to capture
	// the leaf cert the server currently presents.
	newConn := func() (*grpc.ClientConn, *servedCertRecorder, error) {
		rec := &servedCertRecorder{}
		creds := credentials.NewTLS(&tls.Config{
			RootCAs:          caPool,
			ServerName:       "buffnet",
			MinVersion:       tls.VersionTLS12,
			VerifyConnection: rec.verifyConnection,
		})
		conn, err := srv.NewClient(
			grpc.WithTransportCredentials(creds),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  1 * time.Second,
					Multiplier: 2,
					MaxDelay:   15 * time.Second,
				},
			}),
		)
		return conn, rec, err
	}

	rel := tuple.ToV1Relationship(tuple.MustParse(tf.StandardRelationships[0]))

	checkPermission := func(client v1.PermissionsServiceClient) error {
		_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_AtLeastAsFresh{
					AtLeastAsFresh: zedtoken.MustNewFromRevisionForTesting(revision, datalayer.NoSchemaHashInLegacyZedToken),
				},
			},
			Resource:   rel.Resource,
			Permission: "viewer",
			Subject:    rel.Subject,
		})
		return err
	}

	conn, rec, err := newConn()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	require.NoError(t, checkPermission(v1.NewPermissionsServiceClient(conn)))
	served := rec.lastServedCert()
	require.NotNil(t, served)
	require.Equal(t, oldCert.SerialNumber, served.SerialNumber, "expected the initial certificate to be served before rotation")

	// rotate the key
	newCert := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"rotatedTestCert"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(5 * time.Minute),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{"buffnet"},
	}
	newCertPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	newCertPublicKey := &newCertPrivateKey.PublicKey
	newCertBytes, err := x509.CreateCertificate(rand.Reader, newCert, caCert, newCertPublicKey, caPrivateKey)
	require.NoError(t, err)
	newCertParsed, err := x509.ParseCertificate(newCertBytes)
	require.NoError(t, err)

	keyFile, err = os.OpenFile(keyFile.Name(), os.O_WRONLY|os.O_TRUNC, 0o755) //nolint:gosec  // path traversal isn't a problem in tests
	require.NoError(t, err)
	newKeyBytes, err := x509.MarshalECPrivateKey(newCertPrivateKey)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyFile, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: newKeyBytes,
	}))
	require.NoError(t, keyFile.Close())

	certFile, err = os.OpenFile(certFile.Name(), os.O_WRONLY|os.O_TRUNC, 0o755) //nolint:gosec  // path traversal isn't a problem in tests
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: newCertParsed.Raw,
	}))
	require.NoError(t, certFile.Close())

	// Wait for the server to reload the rotated certificate. Each iteration dials
	// a fresh connection to force a new TLS handshake — a reused connection keeps
	// its existing session and would never re-present the cert. This assertion
	// fails if hot reloading in updateCachedCertificate is disabled.
	require.Eventually(t, func() bool {
		rotatedConn, rotatedRec, err := newConn()
		if err != nil {
			t.Logf("newConn failed: %v", err)
			return false
		}
		defer func() { _ = rotatedConn.Close() }()

		if err := checkPermission(v1.NewPermissionsServiceClient(rotatedConn)); err != nil {
			t.Logf("checkPermission failed: %v", err)
			return false
		}

		served := rotatedRec.lastServedCert()
		return served != nil && served.SerialNumber.Cmp(newCertParsed.SerialNumber) == 0
	}, 30*time.Second, 500*time.Millisecond, "server never served the rotated certificate")

	cancel()
	select {
	case err := <-wait:
		require.NoError(t, err)
		return
	case <-time.After(30 * time.Second):
		require.Fail(t, "ungraceful server termination")
	}
}
