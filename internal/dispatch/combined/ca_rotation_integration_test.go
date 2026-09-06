//go:build image

package combined_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"

	"github.com/authzed/spicedb/pkg/testutil/sdbtestcontainer"
)

const (
	// sdbNetworkAlias is the name the node dispatches to, so it is also the name
	// the serving certificate has to be issued for.
	sdbNetworkAlias = "sdb"

	caPath          = "/certs/ca.crt"
	servingCertPath = "/certs/tls.crt"
	servingKeyPath  = "/certs/tls.key"

	rotationSchema = `definition user {}

definition group {
	relation member: user
}

definition document {
	relation viewer: group#member
	permission view = viewer
}`
)

// TestDispatchFollowsCARotation rotates the certificate authority a running
// node uses to verify its dispatch peers, and asserts dispatch recovers without
// a restart.
//
// The node dispatches to itself over the cluster gRPC port, so one container
// covers both sides of the handshake: the serving certificate comes from
// --dispatch-cluster-tls-cert-path and the trust bundle from
// --dispatch-upstream-ca-path, exactly as an operator mounts them from one
// secret.
func TestDispatchFollowsCARotation(t *testing.T) {
	image := envOr("SPICEDB_IMAGE", "authzed/spicedb:ci")

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()

	net, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = net.Remove(t.Context()) })

	authority := newTestAuthority(t, "old")
	certPEM, keyPEM := authority.issueServingCert(t, sdbNetworkAlias)

	spicedb, err := sdbtestcontainer.Run(ctx, image,
		sdbtestcontainer.WithBootstrapSchema(rotationSchema),
		sdbtestcontainer.WithBootstrapRelationships(
			"group:everyone#member@user:alice",
			"document:firstdoc#viewer@group:everyone#member",
		),
		network.WithNetwork([]string{sdbNetworkAlias}, net),
		testcontainers.WithFiles(
			testcontainers.ContainerFile{Reader: bytes.NewReader(authority.bundlePEM(t)), ContainerFilePath: caPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: bytes.NewReader(certPEM), ContainerFilePath: servingCertPath, FileMode: 0o644},
			testcontainers.ContainerFile{Reader: bytes.NewReader(keyPEM), ContainerFilePath: servingKeyPath, FileMode: 0o644},
		),
		testcontainers.WithEnv(map[string]string{
			// Dispatch to ourselves over TLS on the cluster port, so every
			// subproblem crosses a real handshake against the trust bundle.
			"SPICEDB_DISPATCH_CLUSTER_ENABLED":       "true",
			"SPICEDB_DISPATCH_CLUSTER_TLS_CERT_PATH": servingCertPath,
			"SPICEDB_DISPATCH_CLUSTER_TLS_KEY_PATH":  servingKeyPath,
			"SPICEDB_DISPATCH_UPSTREAM_ADDR":         sdbNetworkAlias + ":50053",
			"SPICEDB_DISPATCH_UPSTREAM_CA_PATH":      caPath,
			// Rotation only takes effect on a new connection, and the dispatch
			// client holds one open. A short max age has the server retire it
			// promptly, which is what an operator gets from the 30s default.
			"SPICEDB_DISPATCH_CLUSTER_MAX_CONN_AGE": "5s",
			// Without this a repeated check is answered from cache and never
			// dispatches, so the probe below would stop observing the handshake.
			"SPICEDB_DISPATCH_CACHE_ENABLED":         "false",
			"SPICEDB_DISPATCH_CLUSTER_CACHE_ENABLED": "false",
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = spicedb.Terminate(t.Context()) })

	client, err := authzed.NewClient(spicedb.GRPCEndpoint(),
		grpcutil.WithInsecureBearerToken(spicedb.PresharedKey()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	// The dispatch client waits for its connection to be ready, so a check made
	// while the handshake is failing blocks until its own deadline. The timeout
	// therefore sets how long each probe below takes to report a broken cluster.
	check := func(timeout time.Duration) error {
		checkCtx, cancelCheck := context.WithTimeout(ctx, timeout)
		defer cancelCheck()

		resp, err := client.CheckPermission(checkCtx, &v1.CheckPermissionRequest{
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true},
			},
			Resource:   &v1.ObjectReference{ObjectType: "document", ObjectId: "firstdoc"},
			Permission: "view",
			Subject:    &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "alice"}},
		})
		if err != nil {
			return err
		}
		if resp.Permissionship != v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			return fmt.Errorf("check returned %s, expected HAS_PERMISSION", resp.Permissionship)
		}
		return nil
	}

	require.NoError(t, check(20*time.Second), "dispatch over TLS should work before anything rotates")

	// Publish a serving certificate from a new authority while leaving the trust
	// bundle alone. Dispatch has to break here, otherwise the recovery below
	// would pass whether or not the trust bundle is ever re-read.
	rotated := newTestAuthority(t, "new")
	rotatedCertPEM, rotatedKeyPEM := rotated.issueServingCert(t, sdbNetworkAlias)
	copyToContainer(ctx, t, spicedb, servingKeyPath, rotatedKeyPEM, 0o644)
	copyToContainer(ctx, t, spicedb, servingCertPath, rotatedCertPEM, 0o644)

	brokeAt := time.Now()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Error(c, check(3*time.Second))
	}, 2*time.Minute, time.Second,
		"dispatch should fail while the peer serves a certificate from an authority that is not in the trust bundle")
	t.Logf("dispatch started failing %s after the serving certificate rotated", time.Since(brokeAt).Round(time.Second))

	// Publish the rotated authority, as cert-manager does when it renews the CA.
	copyToContainer(ctx, t, spicedb, caPath, rotated.bundlePEM(t), 0o644)

	publishedAt := time.Now()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, check(3*time.Second))
	}, 2*time.Minute, time.Second,
		"dispatch should recover once the rotated authority is published, without restarting the node")
	t.Logf("dispatch recovered %s after the rotated authority was published", time.Since(publishedAt).Round(time.Second))
}

func copyToContainer(ctx context.Context, t *testing.T, ctr *sdbtestcontainer.Container, path string, contents []byte, mode int64) {
	t.Helper()
	require.NoError(t, ctr.CopyToContainer(ctx, contents, path, mode))
}

// testAuthority is a certificate authority that can sign serving certificates.
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

func (a *testAuthority) bundlePEM(t *testing.T) []byte {
	t.Helper()
	buf := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.cert.Raw})
	require.NotNil(t, buf)
	return buf
}

// issueServingCert signs a certificate for dnsName, returning it and its key in PEM form.
func (a *testAuthority) issueServingCert(t *testing.T, dnsName string) (certPEM, keyPEM []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{Organization: []string{"leaf"}},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{dnsName},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	require.NoError(t, err)

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
