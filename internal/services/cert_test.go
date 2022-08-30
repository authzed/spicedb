package services_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	"github.com/authzed/spicedb/internal/middleware/consistency"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/middleware/servicespecific"
	tf "github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/util"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

func TestCertRotation(t *testing.T) {
	const (
		// length of time the initial cert is valid
		initialValidDuration = 1 * time.Second
		// continue making requests for waitFactor*initialValidDuration
		waitFactor = 3
	)

	certDir, err := os.MkdirTemp("", "test-certs-")
	require.NoError(t, err)

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
	t.Cleanup(func() {
		caFile.Close()
	})
	require.NoError(t, pem.Encode(caFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Raw,
	}))

	old := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"initialTestCert"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(initialValidDuration),
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
	emptyDS, err := memdb.NewMemdbDatastore(0, 10, time.Duration(90_000_000_000_000))
	require.NoError(t, err)
	ds, revision := tf.StandardDatastoreWithData(emptyDS, require.New(t))
	srv, err := server.NewConfigWithOptions(
		server.WithDatastore(ds),
		server.WithDispatcher(graph.NewLocalOnlyDispatcher(1)),
		server.WithDispatchMaxDepth(50),
		server.WithMaximumPreconditionCount(1000),
		server.WithMaximumUpdatesPerWrite(1000),
		server.WithGRPCServer(util.GRPCServerConfig{
			Network:      util.BufferedNetwork,
			Enabled:      true,
			TLSCertPath:  certFile.Name(),
			TLSKeyPath:   keyFile.Name(),
			ClientCAPath: caFile.Name(),
		}),
		server.WithGRPCAuthFunc(func(ctx context.Context) (context.Context, error) {
			return ctx, nil
		}),
		server.WithHTTPGateway(util.HTTPServerConfig{Enabled: false}),
		server.WithDashboardAPI(util.HTTPServerConfig{Enabled: false}),
		server.WithMetricsAPI(util.HTTPServerConfig{Enabled: false}),
		server.WithDispatchServer(util.GRPCServerConfig{Enabled: false}),
	).Complete()
	require.NoError(t, err)
	srv.SetMiddleware([]grpc.UnaryServerInterceptor{
		datastoremw.UnaryServerInterceptor(ds),
		consistency.UnaryServerInterceptor(),
		servicespecific.UnaryServerInterceptor,
	}, []grpc.StreamServerInterceptor{
		datastoremw.StreamServerInterceptor(ds),
		consistency.StreamServerInterceptor(),
		servicespecific.StreamServerInterceptor,
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		require.NoError(t, srv.Run(ctx))
	}()

	conn, err := srv.GRPCDialContext(ctx, grpc.WithReturnConnectionError())
	require.NoError(t, err)
	t.Cleanup(func() {
		if conn != nil {
			require.NoError(t, conn.Close())
		}
		cancel()
	})

	// requests work with the old key
	client := v1.NewPermissionsServiceClient(conn)
	rel := tuple.MustToRelationship(tuple.Parse(tf.StandardTuples[0]))
	_, err = client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
		Consistency: &v1.Consistency{
			Requirement: &v1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: zedtoken.NewFromRevision(revision),
			},
		},
		Resource:   rel.Resource,
		Permission: "viewer",
		Subject:    rel.Subject,
	})
	require.NoError(t, err)

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

	keyFile, err = os.OpenFile(keyFile.Name(), os.O_WRONLY|os.O_TRUNC, 0o755)
	require.NoError(t, err)
	newKeyBytes, err := x509.MarshalECPrivateKey(newCertPrivateKey)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyFile, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: newKeyBytes,
	}))
	require.NoError(t, keyFile.Close())

	certFile, err = os.OpenFile(certFile.Name(), os.O_WRONLY|os.O_TRUNC, 0o755)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: newCertParsed.Raw,
	}))
	require.NoError(t, certFile.Close())

	// check for three seconds (initial cert is only valid for 1 second)
	for i := 0; i < waitFactor; i++ {
		_, err = client.CheckPermission(context.Background(), &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_AtLeastAsFresh{
					AtLeastAsFresh: zedtoken.NewFromRevision(revision),
				},
			},
			Resource:   rel.Resource,
			Permission: "viewer",
			Subject:    rel.Subject,
		})
		require.NoError(t, err)
		time.Sleep(initialValidDuration)
	}
}
