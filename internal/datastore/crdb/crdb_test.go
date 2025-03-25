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
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/internal/datastore/crdb/version"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/migrate"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	veryLargeGCWindow   = 90000 * time.Second
	veryLargeGCInterval = 90000 * time.Second
)

// Implement the TestableDatastore interface
func (cds *crdbDatastore) ExampleRetryableError() error {
	return &pgconn.PgError{
		Code: pool.CrdbRetryErrCode,
	}
}

func crdbTestVersion() string {
	ver := os.Getenv("CRDB_TEST_VERSION")
	if ver != "" {
		return ver
	}

	return version.LatestTestedCockroachDBVersion
}

func TestCRDBDatastoreWithoutIntegrity(t *testing.T) {
	t.Parallel()
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())
	test.All(t, test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ctx := context.Background()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(
				ctx,
				uri,
				GCWindow(gcWindow),
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				OverlapStrategy(overlapStrategyPrefix),
				DebugAnalyzeBeforeStatistics(),
			)
			require.NoError(t, err)
			return ds
		})

		return ds, nil
	}), false)

	t.Run("TestWatchStreaming", createDatastoreTest(
		b,
		StreamingWatchTest,
		RevisionQuantization(0),
		GCWindow(veryLargeGCWindow),
	))
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(ctx, uri, options...)
			require.NoError(t, err)
			return ds
		})
		defer ds.Close()

		tf(t, ds)
	}
}

func TestCRDBDatastoreWithFollowerReads(t *testing.T) {
	t.Parallel()
	followerReadDelay := time.Duration(4.8 * float64(time.Second))
	gcWindow := 100 * time.Second

	engine := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	quantizationDurations := []time.Duration{
		0 * time.Second,
		100 * time.Millisecond,
	}
	for _, quantization := range quantizationDurations {
		t.Run(fmt.Sprintf("Quantization%s", quantization), func(t *testing.T) {
			require := require.New(t)
			ctx := context.Background()

			ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := NewCRDBDatastore(
					ctx,
					uri,
					GCWindow(gcWindow),
					RevisionQuantization(quantization),
					FollowerReadDelay(followerReadDelay),
					DebugAnalyzeBeforeStatistics(),
				)
				require.NoError(err)
				return ds
			})
			defer ds.Close()

			r, err := ds.ReadyState(ctx)
			require.NoError(err)
			require.True(r.IsReady)

			// Revisions should be at least the follower read delay amount in the past
			for start := time.Now(); time.Since(start) < 50*time.Millisecond; {
				testRevision, err := ds.OptimizedRevision(ctx)
				require.NoError(err)

				nowRevision, err := ds.HeadRevision(ctx)
				require.NoError(err)

				diff := nowRevision.(revisions.HLCRevision).TimestampNanoSec() - testRevision.(revisions.HLCRevision).TimestampNanoSec()
				require.True(diff > followerReadDelay.Nanoseconds())
			}
		})
	}
}

var defaultKeyForTesting = proxy.KeyConfig{
	ID: "defaultfortest",
	Bytes: (func() []byte {
		b, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000")
		if err != nil {
			panic(err)
		}
		return b
	})(),
	ExpiredAt: nil,
}

func TestCRDBDatastoreWithIntegrity(t *testing.T) {
	t.Parallel()
	b := testdatastore.RunCRDBForTesting(t, "", crdbTestVersion())

	test.All(t, test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ctx := context.Background()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(
				ctx,
				uri,
				GCWindow(gcWindow),
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				OverlapStrategy(overlapStrategyPrefix),
				DebugAnalyzeBeforeStatistics(),
				WithIntegrity(true),
			)
			require.NoError(t, err)

			wrapped, err := proxy.NewRelationshipIntegrityProxy(ds, defaultKeyForTesting, nil)
			require.NoError(t, err)
			return wrapped
		})

		return ds, nil
	}), false)

	unwrappedTester := test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ctx := context.Background()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(
				ctx,
				uri,
				GCWindow(gcWindow),
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				OverlapStrategy(overlapStrategyPrefix),
				DebugAnalyzeBeforeStatistics(),
				WithIntegrity(true),
			)
			require.NoError(t, err)
			return ds
		})

		return ds, nil
	})

	t.Run("TestRelationshipIntegrityInfo", func(t *testing.T) { RelationshipIntegrityInfoTest(t, unwrappedTester) })
	t.Run("TestBulkRelationshipIntegrityInfo", func(t *testing.T) { BulkRelationshipIntegrityInfoTest(t, unwrappedTester) })
	t.Run("TestWatchRelationshipIntegrity", func(t *testing.T) { RelationshipIntegrityWatchTest(t, unwrappedTester) })
}

func TestWatchFeatureDetection(t *testing.T) {
	t.Parallel()
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
			expectMessage: "(SQLSTATE 42501)",
		},
		{
			name: "rangefeeds enabled, user has permission",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err = adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
				require.NoError(t, err)

				_, err = adminConn.Exec(ctx, fmt.Sprintf(`GRANT CHANGEFEED ON TABLE testspicedb.%s TO unprivileged;`, schema.TableTuple))
				require.NoError(t, err)

				_, err = adminConn.Exec(ctx, fmt.Sprintf(`GRANT SELECT ON TABLE testspicedb.%s TO unprivileged;`, schema.TableTuple))
				require.NoError(t, err)
			},
			expectEnabled: true,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			adminConn, connStrings := newCRDBWithUser(t, pool)
			require.NoError(t, err)

			migrationDriver, err := crdbmigrations.NewCRDBDriver(connStrings[testuser])
			require.NoError(t, err)
			require.NoError(t, crdbmigrations.CRDBMigrations.Run(ctx, migrationDriver, migrate.Head, migrate.LiveRun))

			tt.postInit(ctx, adminConn)

			ds, err := NewCRDBDatastore(ctx, connStrings[unprivileged])
			require.NoError(t, err)

			features, err := ds.Features(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectEnabled, features.Watch.Status == datastore.FeatureSupported)
			require.Contains(t, features.Watch.Reason, tt.expectMessage)

			if features.Watch.Status != datastore.FeatureSupported {
				headRevision, err := ds.HeadRevision(ctx)
				require.NoError(t, err)

				_, errChan := ds.Watch(ctx, headRevision, datastore.WatchJustRelationships())
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
		Repository: "mirror.gcr.io/cockroachdb/cockroach",
		Tag:        "v" + crdbTestVersion(),
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

func RelationshipIntegrityInfoTest(t *testing.T, tester test.DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write a relationship with integrity information.
	timestamp := time.Now().UTC()

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		tpl := tuple.MustParse("document:foo#viewer@user:tom")
		tpl.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "key1",
			Hash:     []byte("hash1"),
			HashedAt: timestamppb.New(timestamp),
		}
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tpl),
		})
	})
	require.NoError(err)

	// Read the relationship back and ensure the integrity information is present.
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"foo"},
		OptionalResourceRelation: "viewer",
	})
	require.NoError(err)

	slice, err := datastore.IteratorToSlice(iter)
	require.NoError(err)

	rel := slice[0]

	require.NotNil(rel.OptionalIntegrity)
	require.Equal("key1", rel.OptionalIntegrity.KeyId)
	require.Equal([]byte("hash1"), rel.OptionalIntegrity.Hash)

	require.LessOrEqual(math.Abs(float64(timestamp.Sub(rel.OptionalIntegrity.HashedAt.AsTime()).Milliseconds())), 1000.0)
}

type fakeSource struct {
	rel *tuple.Relationship
}

func (f *fakeSource) Next(ctx context.Context) (*tuple.Relationship, error) {
	if f.rel == nil {
		return nil, nil
	}

	tpl := f.rel
	f.rel = nil
	return tpl, nil
}

func BulkRelationshipIntegrityInfoTest(t *testing.T, tester test.DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write a relationship with integrity information.
	timestamp := time.Now().UTC()

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rel := tuple.MustParse("document:foo#viewer@user:tom")
		rel.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "key1",
			Hash:     []byte("hash1"),
			HashedAt: timestamppb.New(timestamp),
		}

		_, err := rwt.BulkLoad(ctx, &fakeSource{&rel})
		return err
	})
	require.NoError(err)

	// Read the relationship back and ensure the integrity information is present.
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	reader := ds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"foo"},
		OptionalResourceRelation: "viewer",
	})
	require.NoError(err)

	slice, err := datastore.IteratorToSlice(iter)
	require.NoError(err)

	rel := slice[0]

	require.NotNil(rel.OptionalIntegrity)
	require.Equal("key1", rel.OptionalIntegrity.KeyId)
	require.Equal([]byte("hash1"), rel.OptionalIntegrity.Hash)

	require.LessOrEqual(math.Abs(float64(timestamp.Sub(rel.OptionalIntegrity.HashedAt.AsTime()).Milliseconds())), 1000.0)
}

func RelationshipIntegrityWatchTest(t *testing.T, tester test.DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, rev := testfixtures.StandardDatastoreWithSchema(rawDS, require)
	ctx := context.Background()

	// Write a relationship with integrity information.
	timestamp := time.Now().UTC()

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		rel := tuple.MustParse("document:foo#viewer@user:tom")
		rel.OptionalIntegrity = &core.RelationshipIntegrity{
			KeyId:    "key1",
			Hash:     []byte("hash1"),
			HashedAt: timestamppb.New(timestamp),
		}
		return rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(rel),
		})
	})
	require.NoError(err)

	// Ensure the watch API returns the integrity information.
	opts := datastore.WatchOptions{
		Content:                 datastore.WatchRelationships,
		WatchBufferLength:       128,
		WatchBufferWriteTimeout: 1 * time.Minute,
	}

	changes, errchan := ds.Watch(ctx, rev, opts)
	select {
	case change, ok := <-changes:
		if !ok {
			require.Fail("Timed out waiting for WatchDisconnectedError")
		}

		rel := change.RelationshipChanges[0].Relationship
		require.NotNil(rel.OptionalIntegrity)
		require.Equal("key1", rel.OptionalIntegrity.KeyId)
		require.Equal([]byte("hash1"), rel.OptionalIntegrity.Hash)

		require.LessOrEqual(math.Abs(float64(timestamp.Sub(rel.OptionalIntegrity.HashedAt.AsTime()).Milliseconds())), 1000.0)
	case err := <-errchan:
		require.Failf("Failed waiting for changes with error", "error: %v", err)
	case <-time.NewTimer(10 * time.Second).C:
		require.Fail("Timed out")
	}
}

func StreamingWatchTest(t *testing.T, rawDS datastore.Datastore) {
	require := require.New(t)

	ds, rev := testfixtures.DatastoreFromSchemaAndTestRelationships(rawDS, `
		caveat somecaveat(somecondition int) {
			somecondition == 42	
		}

		caveat somecaveat2(somecondition int) {
			somecondition == 42	
		}

		definition user {}

		definition user2 {}

		definition resource {
			relation viewer: user
		}

		definition resource2 {
			relation viewer: user2
		}
	`, []tuple.Relationship{
		tuple.MustParse("resource:foo#viewer@user:tom"),
		tuple.MustParse("resource:foo#viewer@user:fred"),
	}, require)
	ctx := context.Background()

	// Touch and delete some relationships, add a namespace and caveat and delete a namespace and caveat.
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Delete(tuple.MustParse("resource:foo#viewer@user:fred")),
		})
		require.NoError(err)

		err = rwt.DeleteNamespaces(ctx, "resource2")
		require.NoError(err)

		err = rwt.DeleteCaveats(ctx, []string{"somecaveat2"})
		require.NoError(err)

		err = rwt.WriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
		})
		require.NoError(err)

		err = rwt.WriteCaveats(ctx, []*core.CaveatDefinition{{
			Name: "somenewcaveat",
		}})
		require.NoError(err)

		return nil
	})
	require.NoError(err)

	// Ensure the watch API returns the integrity information.
	opts := datastore.WatchOptions{
		Content:                 datastore.WatchRelationships | datastore.WatchSchema | datastore.WatchCheckpoints,
		WatchBufferLength:       128,
		WatchBufferWriteTimeout: 1 * time.Minute,
		EmissionStrategy:        datastore.EmitImmediatelyStrategy,
	}

	expectedChanges := mapz.NewSet[string]()
	expectedChanges.Add("DELETE(resource:foo#viewer@user:fred)\n")
	expectedChanges.Add("DeletedCaveat: somecaveat2\n")
	expectedChanges.Add("DeletedNamespace: resource2\n")
	expectedChanges.Add("Definition: *corev1.NamespaceDefinition:somenewnamespace\n")
	expectedChanges.Add("Definition: *corev1.CaveatDefinition:somenewcaveat\n")

	changes, errchan := ds.Watch(ctx, rev, opts)
	for {
		select {
		case change, ok := <-changes:
			if !ok {
				require.Fail("Timed out waiting for WatchDisconnectedError")
			}

			debugString := change.DebugString()
			require.True(expectedChanges.Has(debugString), "unexpected change: %s", debugString)
			expectedChanges.Delete(change.DebugString())
			if expectedChanges.IsEmpty() {
				return
			}
		case err := <-errchan:
			require.Failf("Failed waiting for changes with error", "error: %v", err)
		case <-time.NewTimer(10 * time.Second).C:
			require.Fail("Timed out")
		}
	}
}
