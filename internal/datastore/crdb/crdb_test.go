//go:build ci && docker
// +build ci,docker

package crdb

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revision"
	"github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/migrate"
)

// Implement the TestableDatastore interface
func (cds *crdbDatastore) ExampleRetryableError() error {
	return &pgconn.PgError{
		Code: pool.CrdbRetryErrCode,
	}
}

func TestCRDBDatastore(t *testing.T) {
	b := testdatastore.RunCRDBForTesting(t, "")
	test.All(t, test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(
				uri,
				GCWindow(gcWindow),
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				OverlapStrategy(overlapStrategyPrefix),
			)
			require.NoError(t, err)
			return ds
		})

		return ds, nil
	}))
}

func TestCRDBDatastoreWithFollowerReads(t *testing.T) {
	followerReadDelay := time.Duration(4.8 * float64(time.Second))
	gcWindow := 100 * time.Second

	engine := testdatastore.RunCRDBForTesting(t, "")

	quantizationDurations := []time.Duration{
		0 * time.Second,
		100 * time.Millisecond,
	}
	for _, quantization := range quantizationDurations {
		t.Run(fmt.Sprintf("Quantization%s", quantization), func(t *testing.T) {
			require := require.New(t)

			ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := NewCRDBDatastore(
					uri,
					GCWindow(gcWindow),
					RevisionQuantization(quantization),
					FollowerReadDelay(followerReadDelay),
				)
				require.NoError(err)
				return ds
			})
			defer ds.Close()

			ctx := context.Background()
			r, err := ds.ReadyState(ctx)
			require.NoError(err)
			require.True(r.IsReady)

			// Revisions should be at least the follower read delay amount in the past
			for start := time.Now(); time.Since(start) < 50*time.Millisecond; {
				testRevision, err := ds.OptimizedRevision(ctx)
				require.NoError(err)

				nowRevision, err := ds.HeadRevision(ctx)
				require.NoError(err)

				diff := nowRevision.(revision.Decimal).IntPart() - testRevision.(revision.Decimal).IntPart()
				require.True(diff > followerReadDelay.Nanoseconds())
			}
		})
	}
}

func TestWatchFeatureDetection(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	cases := []struct {
		name          string
		postInit      func(ctx context.Context, adminConn *pgx.Conn)
		expectEnabled bool
		expectMessage string
	}{
		{
			name: "rangefeeds disabled",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err = adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = false;`)
				require.NoError(t, err)
			},
			expectEnabled: false,
			expectMessage: "Range feeds must be enabled in CockroachDB and the user must have permission to create them in order to enable the Watch API: ERROR: rangefeeds require the kv.rangefeed.enabled setting. See",
		},
		{
			name: "rangefeeds enabled, user doesn't have permission",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err = adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
				require.NoError(t, err)
			},
			expectEnabled: false,
			expectMessage: "Range feeds must be enabled in CockroachDB and the user must have permission to create them in order to enable the Watch API: ERROR: user unprivileged does not have CHANGEFEED privilege on relation relation_tuple (SQLSTATE 42501)",
		},
		{
			name: "rangefeeds enabled, user has permission",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err = adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
				require.NoError(t, err)
				_, err = adminConn.Exec(ctx, fmt.Sprintf(`GRANT CHANGEFEED ON TABLE testspicedb.%s TO unprivileged;`, tableTuple))
				require.NoError(t, err)
			},
			expectEnabled: true,
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			adminConn, connStrings := newCRDBWithUser(t, pool)
			require.NoError(t, err)

			migrationDriver, err := crdbmigrations.NewCRDBDriver(connStrings[testuser])
			require.NoError(t, err)
			require.NoError(t, crdbmigrations.CRDBMigrations.Run(ctx, migrationDriver, migrate.Head, migrate.LiveRun))

			tt.postInit(ctx, adminConn)

			ds, err := NewCRDBDatastore(connStrings[unprivileged])
			require.NoError(t, err)

			features, err := ds.Features(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectEnabled, features.Watch.Enabled)
			require.Contains(t, features.Watch.Reason, tt.expectMessage)

			if !features.Watch.Enabled {
				headRevision, err := ds.HeadRevision(ctx)
				require.NoError(t, err)

				_, errChan := ds.Watch(ctx, headRevision)
				err = <-errChan
				require.NotNil(t, err)
				require.Contains(t, err.Error(), "watch is currently disabled")
			}
		})
	}
}

type provisionedUser string

const (
	testuser     provisionedUser = "testuser"
	unprivileged provisionedUser = "unprivileged"
)

func newCRDBWithUser(t *testing.T, pool *dockertest.Pool) (adminConn *pgx.Conn, connStrings map[provisionedUser]string) {
	// in order to create users, cockroach must be running with
	// real certs, and the root user must be authenticated with
	// client certs.
	certDir := t.TempDir()

	ca := &x509.Certificate{
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		SerialNumber:          big.NewInt(0),
		Subject:               pkix.Name{Organization: []string{"Cockroach"}, CommonName: "Cockroach CA"},
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

	certData := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", "node"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	certPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	certPublicKey := &certPrivateKey.PublicKey
	certBytes, err := x509.CreateCertificate(rand.Reader, certData, caCert, certPublicKey, caPrivateKey)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(certBytes)
	require.NoError(t, err)

	keyFile, err := os.OpenFile(filepath.Join(certDir, "node.key"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	keyBytes, err := x509.MarshalECPrivateKey(certPrivateKey)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyFile, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	}))
	require.NoError(t, keyFile.Close())

	certFile, err := os.OpenFile(filepath.Join(certDir, "node.crt"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	}))
	require.NoError(t, certFile.Close())

	rootUserCertData := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Cockroach"},
			CommonName:   "root",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		DNSNames:              []string{"root"},
	}
	rootUserPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	rootUserPublicKey := &rootUserPrivateKey.PublicKey
	rootUserCertBytes, err := x509.CreateCertificate(rand.Reader, rootUserCertData, caCert, rootUserPublicKey, caPrivateKey)
	require.NoError(t, err)
	rootUserCert, err := x509.ParseCertificate(rootUserCertBytes)
	require.NoError(t, err)

	rootKeyFile, err := os.OpenFile(filepath.Join(certDir, "client.root.key"), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	rootKeyBytes, err := x509.MarshalECPrivateKey(rootUserPrivateKey)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(rootKeyFile, &pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: rootKeyBytes,
	}))
	require.NoError(t, rootKeyFile.Close())

	rootCertFile, err := os.Create(filepath.Join(certDir, "client.root.crt"))
	require.NoError(t, err)
	require.NoError(t, pem.Encode(rootCertFile, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: rootUserCert.Raw,
	}))
	require.NoError(t, rootCertFile.Close())

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "cockroachdb/cockroach",
		Tag:        testdatastore.CRDBTestVersionTag,
		Cmd:        []string{"start-single-node", "--certs-dir", "/certs", "--accept-sql-without-tls"},
		Mounts:     []string{certDir + ":/certs"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort(fmt.Sprintf("%d/tcp", 26257))
	require.NoError(t, pool.Retry(func() error {
		var err error
		_, err = pgxpool.New(context.Background(), fmt.Sprintf("postgres://root@localhost:%[1]s/defaultdb?sslmode=verify-full&sslrootcert=%[2]s/ca.crt&sslcert=%[2]s/client.root.crt&sslkey=%[2]s/client.root.key", port, certDir))
		if err != nil {
			t.Log(err)
			return err
		}
		return nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	adminConnString := fmt.Sprintf("postgresql://root:unused@localhost:%[1]s?sslmode=require&sslrootcert=%[2]s/ca.crt&sslcert=%[2]s/client.root.crt&sslkey=%[2]s/client.root.key", port, certDir)

	require.Eventually(t, func() bool {
		adminConn, err = pgx.Connect(ctx, adminConnString)
		return err == nil
	}, 30*time.Second, 1*time.Second)

	// create a non-admin user
	_, err = adminConn.Exec(ctx, `
		CREATE DATABASE testspicedb;
		CREATE USER testuser WITH PASSWORD 'testpass';
		CREATE USER unprivileged WITH PASSWORD 'testpass2';
	`)
	require.NoError(t, err)

	connStrings = map[provisionedUser]string{
		testuser:     fmt.Sprintf("postgresql://testuser:testpass@localhost:%[1]s/testspicedb?sslmode=require&sslrootcert=%[2]s/ca.crt", port, certDir),
		unprivileged: fmt.Sprintf("postgresql://unprivileged:testpass2@localhost:%[1]s/testspicedb?sslmode=require&sslrootcert=%[2]s/ca.crt", port, certDir),
	}

	return
}
