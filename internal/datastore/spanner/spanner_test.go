//go:build ci && docker
// +build ci,docker

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

// Implement TestableDatastore interface
func (sd *spannerDatastore) ExampleRetryableError() error {
	return status.New(codes.Aborted, "retryable").Err()
}

func TestSpannerDatastore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := testdatastore.RunSpannerForTesting(t, "", "head")

	// TODO(jschorr): Once https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/74 has been resolved,
	// change back to `All` to re-enable watch and GC tests.
	// GC tests are disabled because they depend also on the ability to configure change streams with custom retention.
	test.AllWithExceptions(t, test.DatastoreTesterFunc(func(revisionQuantization, _, _ time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewSpannerDatastore(ctx, uri,
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength))
			require.NoError(t, err)
			return ds
		})
		return ds, nil
	}), test.WithCategories(test.GCCategory, test.WatchCategory, test.StatsCategory), true)

	t.Run("TestFakeStats", createDatastoreTest(
		b,
		FakeStatsTest,
	))
}

type datastoreTestFunc func(t *testing.T, ds datastore.Datastore)

func createDatastoreTest(b testdatastore.RunningEngineForTest, tf datastoreTestFunc, options ...Option) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewSpannerDatastore(ctx, uri, options...)
			require.NoError(t, err)
			return ds
		})
		defer ds.Close()

		tf(t, ds)
	}
}

const createFakeStatsTable = `
CREATE TABLE fake_stats_table (
  interval_end TIMESTAMP,
  table_name STRING(MAX),
  used_bytes INT64,
) PRIMARY KEY (table_name, interval_end)
`

func FakeStatsTest(t *testing.T, ds datastore.Datastore) {
	spannerDS := ds.(*spannerDatastore)
	spannerDS.tableSizesStatsTable = "fake_stats_table"

	spannerClient := spannerDS.client
	ctx := context.Background()

	adminClient, err := admin.NewDatabaseAdminClient(ctx)
	require.NoError(t, err)

	// Manually add the stats table to simulate the table that the emulator doesn't create.
	updateOp, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database: spannerClient.DatabaseName(),
		Statements: []string{
			createFakeStatsTable,
		},
	})
	require.NoError(t, err)

	err = updateOp.Wait(ctx)
	require.NoError(t, err)

	// Call stats with no stats rows and no relationship rows.
	stats, err := ds.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), stats.EstimatedRelationshipCount)

	// Add some relationships.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tom")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:sarah")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:fred")),
		})
	})
	require.NoError(t, err)

	// Call stats with no stats rows and some relationship rows.
	stats, err = ds.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), stats.EstimatedRelationshipCount)

	// Add some stats row with a byte count.
	_, err = spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("fake_stats_table", []string{"interval_end", "table_name", "used_bytes"}, []interface{}{
			time.Now().UTC().Add(-100 * time.Second), tableRelationship, 100,
		}),
	})
	require.NoError(t, err)

	// Call stats with a stats row and some relationship rows and ensure we get an estimate.
	stats, err = ds.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.EstimatedRelationshipCount)

	// Add some more relationships.
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Create(tuple.MustParse("document:foo#viewer@user:tommy1236512365123651236512365123612365123655")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:sara1236512365123651236512365123651236512365")),
			tuple.Create(tuple.MustParse("document:foo#viewer@user:freddy1236512365123651236512365123651236512365")),
		})
	})
	require.NoError(t, err)

	// Call stats again and ensure it uses the cached relationship size value, even if we'd addded more relationships.
	stats, err = ds.Statistics(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.EstimatedRelationshipCount)
}
