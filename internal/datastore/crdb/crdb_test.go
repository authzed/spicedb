//go:build datastore

package crdb

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	promclient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/cockroachdb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	crdbmigrations "github.com/authzed/spicedb/internal/datastore/crdb/migrations"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	"github.com/authzed/spicedb/internal/datastore/crdb/schema"
	"github.com/authzed/spicedb/internal/datastore/crdb/version"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/internal/datastore/proxy/indexcheck"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/testfixtures"
	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
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

var crdbFactory = test.NewTesterFactory(&pgconn.PgError{Code: pool.CrdbRetryErrCode})

func crdbTestVersion() string {
	ver := os.Getenv("CRDB_TEST_VERSION")
	if ver != "" {
		return ver
	}

	return version.LatestTestedCockroachDBVersion
}

func TestCRDBDatastoreWithoutIntegrity(t *testing.T) {
	t.Parallel()
	b := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	test.All(t, crdbFactory.NewTester(test.DatastoreTesterFunc(func(t testing.TB, revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ctx := t.Context()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(
				ctx,
				uri,
				GCWindow(gcWindow),
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				OverlapStrategy(overlapStrategyPrefix),
				DebugAnalyzeBeforeStatistics(),
				WithAcquireTimeout(5*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = ds.Close()
			})
			return indexcheck.WrapWithIndexCheckingDatastoreProxyIfApplicable(ds)
		})

		return ds, nil
	})))

	t.Run("TestWatchStreaming", createDatastoreTest(
		b,
		StreamingWatchTest,
		RevisionQuantization(0),
		GCWindow(veryLargeGCWindow),
		WithAcquireTimeout(5*time.Second),
	))

	t.Run("TestTransactionMetadataMarking", createDatastoreTest(
		b,
		TransactionMetadataMarkingTest,
		RevisionQuantization(0),
		GCWindow(veryLargeGCWindow),
		WithAcquireTimeout(5*time.Second),
	))

	t.Run("TestTTLChangefeedSuppressionParam", createDatastoreTest(
		b,
		TTLChangefeedSuppressionParamTest,
		RevisionQuantization(0),
		GCWindow(veryLargeGCWindow),
		WithAcquireTimeout(5*time.Second),
	))

	t.Run("TestTTLChangefeedSuppressionWatch", createDatastoreTest(
		b,
		TTLChangefeedSuppressionWatchTest,
		RevisionQuantization(0),
		GCWindow(veryLargeGCWindow),
		WithAcquireTimeout(5*time.Second),
	))
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ctx := t.Context()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewCRDBDatastore(ctx, uri, options...)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = ds.Close()
			})
			return ds
		})

		tf(t, ds)
	}
}

func TestCRDBDatastoreWithFollowerReads(t *testing.T) {
	followerReadDelay := time.Duration(4.8 * float64(time.Second))
	gcWindow := 100 * time.Second

	engine := testdatastore.RunCRDBForTesting(t, crdbTestVersion())

	quantizationDurations := []time.Duration{
		0 * time.Second,
		100 * time.Millisecond,
	}
	for _, quantization := range quantizationDurations {
		t.Run(fmt.Sprintf("Quantization%s", quantization), func(t *testing.T) {
			ctx := t.Context()

			ds := engine.NewDatastore(t, func(engine, uri string) datastore.Datastore {
				ds, err := NewCRDBDatastore(
					ctx,
					uri,
					GCWindow(gcWindow),
					RevisionQuantization(quantization),
					FollowerReadDelay(followerReadDelay),
					DebugAnalyzeBeforeStatistics(),
					WithAcquireTimeout(5*time.Second),
				)
				require.NoError(t, err)
				return ds
			})
			t.Cleanup(func() {
				_ = ds.Close()
			})

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				r, err := ds.ReadyState(ctx)
				require.NoError(c, err)
				require.True(c, r.IsReady, "datastore not ready: %s", r.Message)
			}, 3*time.Second, 50*time.Millisecond)

			// Revisions should be at least the follower read delay amount in the past
			for start := time.Now(); time.Since(start) < 50*time.Millisecond; {
				testRevisionResult, err := ds.OptimizedRevision(ctx)
				require.NoError(t, err)

				nowRevisionResult, err := ds.HeadRevision(ctx)
				require.NoError(t, err)

				diff := nowRevisionResult.Revision.(revisions.HLCRevision).TimestampNanoSec() - testRevisionResult.Revision.(revisions.HLCRevision).TimestampNanoSec()
				require.Greater(t, diff, followerReadDelay.Nanoseconds())
			}
		})
	}
}

var defaultKeyForTesting = proxy.KeyConfig{
	ID: "defaultfortest",
	Bytes: func() []byte {
		b, err := hex.DecodeString("000102030405060708090A0B0C0D0E0FF0E0D0C0B0A090807060504030201000")
		if err != nil {
			panic(err)
		}
		return b
	}(),
	ExpiredAt: nil,
}

func TestCRDBDatastoreWithIntegrity(t *testing.T) { //nolint:tparallel
	t.Parallel()
	b := testdatastore.RunCRDBForTesting(t, crdbTestVersion())

	test.All(t, crdbFactory.NewTester(test.DatastoreTesterFunc(func(_ testing.TB, revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ctx := t.Context()
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
				WithAcquireTimeout(5*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = ds.Close()
			})

			wrapped, err := proxy.NewRelationshipIntegrityProxy(ds, defaultKeyForTesting, nil)
			require.NoError(t, err)
			return wrapped
		})

		return ds, nil
	})))

	unwrappedTester := test.DatastoreTesterFunc(func(_ testing.TB, revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ctx := t.Context()
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
				WithAcquireTimeout(5*time.Second),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = ds.Close()
			})
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
	cases := []struct {
		name          string
		postInit      func(ctx context.Context, adminConn *pgx.Conn)
		expectEnabled bool
		expectMessage string
	}{
		{
			name: "rangefeeds disabled",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err := adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = false;`)
				require.NoError(t, err)
			},
			expectEnabled: false,
			expectMessage: "Range feeds must be enabled in CockroachDB and the user must have permission to create them in order to enable the Watch API: ERROR: rangefeeds require the kv.rangefeed.enabled setting. See",
		},
		{
			name: "rangefeeds enabled, user doesn't have permission",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err := adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
				require.NoError(t, err)
			},
			expectEnabled: false,
			expectMessage: "(SQLSTATE 42501)",
		},
		{
			name: "rangefeeds enabled, user has permission",
			postInit: func(ctx context.Context, adminConn *pgx.Conn) {
				_, err := adminConn.Exec(ctx, `SET CLUSTER SETTING kv.rangefeed.enabled = true;`)
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
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			adminConn, connStrings := newCRDBWithUser(t)

			migrationDriver, err := crdbmigrations.NewCRDBDriver(connStrings[testuser])
			require.NoError(t, err)
			require.NoError(t, crdbmigrations.CRDBMigrations.Run(ctx, migrationDriver, migrate.Head, migrate.LiveRun))

			tt.postInit(ctx, adminConn)

			// Grant SELECT on schema_revision to unprivileged user so HeadRevision can read schema hashes.
			_, err = adminConn.Exec(ctx, `GRANT SELECT ON TABLE testspicedb.schema_revision TO unprivileged;`)
			require.NoError(t, err)

			ds, err := NewCRDBDatastore(ctx, connStrings[unprivileged], WithAcquireTimeout(5*time.Second))
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = ds.Close()
			})

			features, err := ds.Features(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expectEnabled, features.Watch.Status == datastore.FeatureSupported)
			require.Contains(t, features.Watch.Reason, tt.expectMessage)

			if features.Watch.Status != datastore.FeatureSupported {
				headRevisionResult, err := ds.HeadRevision(ctx)
				require.NoError(t, err)

				_, errChan := ds.Watch(ctx, headRevisionResult.Revision, datastore.WatchJustRelationships())
				err = <-errChan
				require.Error(t, err)
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

func newCRDBWithUser(t *testing.T) (adminConn *pgx.Conn, connStrings map[provisionedUser]string) {
	container, err := cockroachdb.Run(
		t.Context(),
		"mirror.gcr.io/cockroachdb/cockroach:v"+crdbTestVersion(),
	)
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		config, err := container.ConnectionConfig(t.Context())
		if !assert.NoError(t, err) {
			return
		}
		adminConn, err = pgx.ConnectConfig(t.Context(), config)
		assert.NoError(collect, err)
	}, 5*time.Second, 1*time.Second)

	// create a non-admin user
	_, err = adminConn.Exec(t.Context(), `
		CREATE DATABASE testspicedb;
		CREATE USER testuser WITH PASSWORD 'testpass';
		CREATE USER unprivileged WITH PASSWORD 'testpass2';
	`)
	require.NoError(t, err)

	host, err := container.Host(t.Context())
	require.NoError(t, err)
	port, err := container.MappedPort(t.Context(), "26257/tcp")
	require.NoError(t, err)
	hostAndPort := net.JoinHostPort(host, port.Port())

	connStrings = map[provisionedUser]string{
		testuser:     fmt.Sprintf("postgresql://testuser:testpass@%[1]s/testspicedb?sslmode=require", hostAndPort),
		unprivileged: fmt.Sprintf("postgresql://unprivileged:testpass2@%[1]s/testspicedb?sslmode=require", hostAndPort),
	}

	return adminConn, connStrings
}

func RelationshipIntegrityInfoTest(t *testing.T, tester test.DatastoreTester) {
	require := require.New(t)

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)
	ctx := t.Context()

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
	headRevResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	headRev := headRevResult.Revision

	reader := ds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"foo"},
		OptionalResourceRelation: "viewer",
	}, options.WithQueryShape(queryshape.AllSubjectsForResources))
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

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithSchema(t, rawDS)
	ctx := t.Context()

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
	headRevResult, err := ds.HeadRevision(ctx)
	require.NoError(err)
	headRev := headRevResult.Revision

	reader := ds.SnapshotReader(headRev)
	iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{
		OptionalResourceType:     "document",
		OptionalResourceIds:      []string{"foo"},
		OptionalResourceRelation: "viewer",
	}, options.WithQueryShape(queryshape.AllSubjectsForResources))
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

	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	ds, rev := testfixtures.StandardDatastoreWithSchema(t, rawDS)
	ctx := t.Context()

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

func TransactionMetadataMarkingTest(t *testing.T, rawDS datastore.Datastore) {
	require := require.New(t)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, `
		use expiration
		definition user {}

		definition resource {
			relation viewer: user | user with expiration
		}
	`, []tuple.Relationship{
		tuple.MustParse("resource:foo#viewer@user:tom"),
		tuple.MustParse("resource:foo#viewer@user:fred"),
	})
	ctx := t.Context()

	cds := datastore.UnwrapAs[*crdbDatastore](ds)
	require.NotNil(cds)

	// Ensure the transaction metadata table is empty.
	err := cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var count int
			err := rows.Scan(&count)
			require.NoError(err)
			require.Equal(0, count)
		}
		return nil
	}, fmt.Sprintf("SELECT COUNT(*) FROM %s", schema.TableTransactionMetadata))
	require.NoError(err)

	// Write some rels without expiration, which should still avoid writing to the transactions table.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Touch(tuple.MustParse("resource:foo#viewer@user:fred")),
		})
		require.NoError(err)
		return nil
	})
	require.NoError(err)

	// Ensure the transaction metadata table is still empty.
	err = cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var count int
			err := rows.Scan(&count)
			require.NoError(err)
			require.Equal(0, count)
		}
		return nil
	}, fmt.Sprintf("SELECT COUNT(*) FROM %s", schema.TableTransactionMetadata))
	require.NoError(err)

	// Only delete rels; with TTL deletes suppressed at the changefeed level,
	// this must NOT result in a transaction metadata entry.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Delete(tuple.MustParse("resource:foo#viewer@user:fred")),
		})
		require.NoError(err)
		return nil
	})
	require.NoError(err)

	err = cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var count int
			err := rows.Scan(&count)
			require.NoError(err)
			require.Equal(0, count)
		}
		return nil
	}, fmt.Sprintf("SELECT COUNT(*) FROM %s", schema.TableTransactionMetadata))
	require.NoError(err)

	// Write some rels with user-supplied metadata, which is the only case that
	// results in a transaction metadata entry.
	metadata, err := structpb.NewStruct(map[string]any{
		"key1": "value1",
	})
	require.NoError(err)

	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("resource:foo#viewer@user:fred")),
		})
		require.NoError(err)
		return nil
	}, options.WithMetadata(metadata))
	require.NoError(err)

	err = cds.readPool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var count int
			err := rows.Scan(&count)
			require.NoError(err)
			require.Equal(1, count)
		}
		return nil
	}, fmt.Sprintf("SELECT COUNT(*) FROM %s", schema.TableTransactionMetadata))
	require.NoError(err)
}

func TTLChangefeedSuppressionParamTest(t *testing.T, ds datastore.Datastore) {
	require := require.New(t)
	ctx := t.Context()

	cds := datastore.UnwrapAs[*crdbDatastore](ds)
	require.NotNil(cds)

	// The datastore constructor should have set ttl_disable_changefeed_replication
	// on both relationship tables (CRDB under test is >= 24.1).
	for _, tableName := range []string{schema.TableTuple, schema.TableTupleWithIntegrity} {
		var createStatement string
		err := cds.readPool.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
			return row.Scan(&createStatement)
		}, fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s]", tableName))
		require.NoError(err)
		require.Contains(createStatement, "ttl_disable_changefeed_replication",
			"expected %s to have ttl_disable_changefeed_replication set", tableName)
	}
}

func TTLChangefeedSuppressionWatchTest(t *testing.T, rawDS datastore.Datastore) {
	require := require.New(t)

	ds, _ := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, `
		definition user {}

		definition resource {
			relation viewer: user
		}
	`, []tuple.Relationship{
		tuple.MustParse("resource:first#viewer@user:tom"),
		tuple.MustParse("resource:second#viewer@user:fred"),
	})
	ctx := t.Context()

	cds := datastore.UnwrapAs[*crdbDatastore](ds)
	require.NotNil(cds)

	headRev, err := ds.HeadRevision(ctx)
	require.NoError(err)

	changes, errchan := ds.Watch(ctx, headRev.Revision, datastore.WatchOptions{
		Content:            datastore.WatchRelationships,
		CheckpointInterval: 100 * time.Millisecond,
	})

	// Delete a row in a session with changefeed replication disabled. This sets
	// the same OmitInRangefeeds transaction flag that CRDB's row-level TTL job
	// sets when ttl_disable_changefeed_replication is enabled on the table, so
	// the delete must NOT be emitted by the Watch API.
	conn, err := pgx.Connect(ctx, cds.dburl)
	require.NoError(err)
	_, err = conn.Exec(ctx, "SET disable_changefeed_replication = true")
	require.NoError(err)
	tag, err := conn.Exec(ctx, "DELETE FROM "+cds.schema.RelationshipTableName+" WHERE object_id = 'first'")
	require.NoError(err)
	require.Equal(int64(1), tag.RowsAffected())
	require.NoError(conn.Close(ctx))

	// Then delete a row normally; it MUST be emitted. Receiving it also proves
	// the suppressed delete (which committed earlier) was skipped rather than
	// still in flight.
	conn2, err := pgx.Connect(ctx, cds.dburl)
	require.NoError(err)
	tag, err = conn2.Exec(ctx, "DELETE FROM "+cds.schema.RelationshipTableName+" WHERE object_id = 'second'")
	require.NoError(err)
	require.Equal(int64(1), tag.RowsAffected())
	require.NoError(conn2.Close(ctx))

	timeout := time.After(30 * time.Second)
	for {
		select {
		case change, ok := <-changes:
			require.True(ok, "changes channel closed unexpectedly")
			if change.IsCheckpoint {
				continue
			}
			if len(change.RelationshipChanges) == 0 {
				continue
			}
			// The first (and only) relationship event must be the deletion of
			// resource:second; resource:first was suppressed.
			for _, rc := range change.RelationshipChanges {
				require.Equal(tuple.UpdateOperationDelete, rc.Operation)
				require.Equal("second", rc.Relationship.Resource.ObjectID,
					"the suppressed delete of resource:first leaked into the Watch API")
			}
			return

		case werr := <-errchan:
			require.NoError(werr, "unexpected watch error")

		case <-timeout:
			require.Fail("timed out waiting for the non-suppressed delete event")
		}
	}
}

func StreamingWatchTest(t *testing.T, rawDS datastore.Datastore) {
	require := require.New(t)

	ds, rev := testfixtures.DatastoreFromSchemaAndTestRelationships(t, rawDS, `
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
	})
	ctx := t.Context()

	// Touch and delete some relationships, add a namespace and caveat and delete a namespace and caveat.
	_, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		err := rwt.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("resource:foo#viewer@user:tom")),
			tuple.Delete(tuple.MustParse("resource:foo#viewer@user:fred")),
		})
		require.NoError(err)

		err = rwt.LegacyDeleteNamespaces(ctx, []string{"resource2"}, datastore.DeleteNamespacesAndRelationships)
		require.NoError(err)

		err = rwt.LegacyDeleteCaveats(ctx, []string{"somecaveat2"})
		require.NoError(err)

		err = rwt.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{
			Name: "somenewnamespace",
		})
		require.NoError(err)

		err = rwt.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{{
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

func TestWrapErr(t *testing.T) {
	// this is a sanity check that these errors are correctly passed up
	// unmodified so that higher layers can interpret them - in this case
	// so we can return ResourceExhausted if we see this error.
	require.Equal(t, wrapError(pool.ErrAcquire), pool.ErrAcquire)
}

func TestRegisterPrometheusCollectors(t *testing.T) {
	const (
		readMaxConns  = 10
		writeMaxConns = 20
	)
	// Create read & write pools
	readPoolConfig, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://db:password@pg.example.com:5432/mydb?pool_max_conns=%d", readMaxConns))
	require.NoError(t, err)
	readPool, err := pool.NewRetryPool(t.Context(), "read", readPoolConfig, nil, 18, 20)
	require.NoError(t, err)
	t.Cleanup(func() {
		readPool.Close()
	})

	writePoolConfig, err := pgxpool.ParseConfig(fmt.Sprintf("postgres://db:password@pg.example.com:5432/mydb?pool_max_conns=%d", writeMaxConns))
	require.NoError(t, err)
	writePool, err := pool.NewRetryPool(t.Context(), "read", writePoolConfig, nil, 18, 20)
	require.NoError(t, err)

	// Create datastore with those pools
	cds := &crdbDatastore{readPool: readPool, writePool: writePool, cancel: func() {}}
	t.Cleanup(func() {
		_ = cds.Close()
	})

	err = cds.registerPrometheusCollectors(false)
	require.NoError(t, err)
	require.Empty(t, cds.collectors)

	// Register collectors and verify the values of the metrics
	err = cds.registerPrometheusCollectors(true)
	require.NoError(t, err)
	require.Len(t, cds.collectors, 2)

	metricFamily, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var maxConnsMetricFamily *promclient.MetricFamily
	for _, metric := range metricFamily {
		if metric.GetName() == "pgxpool_max_conns" {
			maxConnsMetricFamily = metric
			break
		}
	}
	require.NotNil(t, maxConnsMetricFamily)
	require.Len(t, maxConnsMetricFamily.GetMetric(), 2)
	metrics := []*promclient.Metric{maxConnsMetricFamily.GetMetric()[0], maxConnsMetricFamily.GetMetric()[1]}

	var poolReadMetric, poolWriteMetric *promclient.Metric

	for _, metric := range metrics {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "pool_usage" {
				switch label.GetValue() {
				case "read":
					poolReadMetric = metric
				case "write":
					poolWriteMetric = metric
				default:
					t.Errorf("unknown label value for pool_usage")
				}
			}
		}
	}

	require.NotNil(t, poolWriteMetric)
	require.Equal(t, float64(writeMaxConns), poolWriteMetric.GetGauge().GetValue()) //nolint:testifylint // we expect exact values
	require.NotNil(t, poolReadMetric)
	require.Equal(t, float64(readMaxConns), poolReadMetric.GetGauge().GetValue()) //nolint:testifylint // we expect exact values
}

func TestVersionReading(t *testing.T) {
	require := require.New(t)

	expectedVersionList := strings.Split(crdbTestVersion(), ".")
	expectedMajor, err := strconv.Atoi(expectedVersionList[0])
	require.NoError(err)
	expectedMinor, err := strconv.Atoi(expectedVersionList[1])
	require.NoError(err)
	expectedPatch, err := strconv.Atoi(expectedVersionList[2])
	require.NoError(err)

	var version crdbVersion

	b := testdatastore.RunCRDBForTesting(t, crdbTestVersion())
	uri := b.NewDatabase(t)

	// Set up a raw connection to the DB
	initPoolConfig, err := pgxpool.ParseConfig(uri)
	require.NoError(err)
	checker, err := pool.NewNodeHealthChecker(uri)
	require.NoError(err)
	initPool, err := pool.NewRetryPool(t.Context(), "pool", initPoolConfig, checker, 18, 20)
	require.NoError(err)
	t.Cleanup(func() {
		initPool.Close()
	})

	// Make the query for the server version
	err = queryServerVersion(t.Context(), initPool, &version)
	require.NoError(err)

	require.Equal(expectedMajor, version.Major)
	require.Equal(expectedMinor, version.Minor)
	require.Equal(expectedPatch, version.Patch)
}
