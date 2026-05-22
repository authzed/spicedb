//go:build datastore

package spanner

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	admin "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
	"github.com/authzed/spicedb/pkg/tuple"
)

var spannerFactory = test.NewTesterFactory(status.New(codes.Aborted, "retryable").Err())

func TestSpannerDatastore(t *testing.T) {
	// t.Parallel() //nolint:tparallel, the test sets environment variables (the emulator)

	ctx := t.Context()
	b := testdatastore.RunSpannerForTesting(t, "", "head")

	// Transaction tests are excluded because, for reasons unknown, one cannot read its own write in one transaction in the Spanner emulator.
	test.AllWithExceptions(t, spannerFactory.NewTester(test.DatastoreTesterFunc(func(_ testing.TB, revisionQuantization, _, _ time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewSpannerDatastore(ctx, uri,
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength),
				WithDatastoreMetricsOption(DatastoreMetricsOptionOpenTelemetry),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, ds.Close())
			})
			return ds
		})
		return ds, nil
	})), test.WithCategories(test.GCCategory, test.StatsCategory, test.TransactionCategory))

	t.Run("TestFakeStats", createDatastoreTest(
		b,
		FakeStatsTest,
	))

	t.Run("TestOptimizedRevisionAfterFreshMigration", createDatastoreTest(
		b,
		OptimizedRevisionAfterFreshMigrationTest,
		FollowerReadDelay(4800*time.Millisecond),
	))
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ctx := t.Context()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewSpannerDatastore(ctx, uri, options...)
			require.NoError(t, err)
			return ds
		})
		defer ds.Close()

		tf(t, ds)
	}
}

// See real table schema in https://docs.cloud.google.com/spanner/docs/introspection/table-sizes-statistics
const createFakeStatsTable = `
CREATE TABLE fake_stats_table (
  interval_end TIMESTAMP,
  table_name STRING(MAX),
  used_bytes FLOAT64,
) PRIMARY KEY (table_name, interval_end)
`

func FakeStatsTest(t *testing.T, ds datastore.Datastore) {
	spannerDS := ds.(*spannerDatastore)
	spannerDS.tableSizesStatsTable = "fake_stats_table"

	spannerClient := spannerDS.client

	adminClient, err := admin.NewDatabaseAdminClient(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = adminClient.Close()
	})

	// Manually add the stats table to simulate the table that the emulator doesn't create.
	updateOp, err := adminClient.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: spannerClient.DatabaseName(),
		Statements: []string{
			createFakeStatsTable,
		},
	})
	require.NoError(t, err)

	err = updateOp.Wait(t.Context())
	require.NoError(t, err)

	// Call stats with no stats rows and no relationship rows.
	stats, err := ds.Statistics(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(0), stats.EstimatedRelationshipCount)

	// Add some relationships.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:sarah")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:fred")),
		})
	})
	require.NoError(t, err)

	// Call stats with no stats rows and some relationship rows.
	stats, err = ds.Statistics(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(0), stats.EstimatedRelationshipCount)

	// Add some stats row with a byte count.
	_, err = spannerClient.Apply(t.Context(), []*spanner.Mutation{
		spanner.Insert("fake_stats_table", []string{"interval_end", "table_name", "used_bytes"}, []any{
			time.Now().UTC().Add(-100 * time.Second), tableRelationship, float64(100),
		}),
	})
	require.NoError(t, err)

	// Call stats with a stats row and some relationship rows and ensure we get an estimate.
	stats, err = ds.Statistics(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.EstimatedRelationshipCount)

	// Add some more relationships.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tommy1236512365123651236512365123612365123655")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:sara1236512365123651236512365123651236512365")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:freddy1236512365123651236512365123651236512365")),
		})
	})
	require.NoError(t, err)

	// Call stats again and ensure it uses the cached relationship size value, even if we'd added more relationships.
	stats, err = ds.Statistics(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.EstimatedRelationshipCount)
}

// OptimizedRevisionAfterFreshMigrationTest verifies that OptimizedRevision
// succeeds when the configured FollowerReadDelay pushes the stale read
// timestamp before the migration that created the schema_revision table.
// This reproduces the case hit by mage testcons:spanner, where the default
// 4.8s FollowerReadDelay lands before the freshly-run migration.
func OptimizedRevisionAfterFreshMigrationTest(t *testing.T, ds datastore.Datastore) {
	result, err := ds.OptimizedRevision(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, datastore.NoRevision, result.Revision)
	require.Empty(t, result.SchemaHash, "schema hash should be empty when no schema has been written yet")
}
