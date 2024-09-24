package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func RelationshipCountersTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))

	// Try calling count without the filter being registered.
	reader := ds.SnapshotReader(rev)

	_, err = reader.CountRelationships(context.Background(), "somefilter")
	require.Error(t, err)
	require.Contains(t, err.Error(), "counter with name `somefilter` not found")

	// Register the filter.
	updatedRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "document", &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.NoError(t, err)

		// Register another filter.
		err = tx.RegisterCounter(ctx, "another", &core.RelationshipFilter{
			ResourceType: testfixtures.FolderNS.Name,
		})
		require.NoError(t, err)

		return nil
	})
	require.NoError(t, err)

	// Try to register again.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "document", &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "counter with name `document` already registered")
		return nil
	})
	require.NoError(t, err)

	// Check the count using the filter.
	reader = ds.SnapshotReader(updatedRev)

	expectedCount := 0
	iter, err := reader.QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	})
	require.NoError(t, err)

	for iter.Next() != nil {
		expectedCount++
		require.NoError(t, iter.Err())
	}
	iter.Close()

	count, err := reader.CountRelationships(context.Background(), "document")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Call another filter.
	expectedCount = 0
	iter, err = reader.QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.FolderNS.Name,
	})
	require.NoError(t, err)

	for iter.Next() != nil {
		expectedCount++
		require.NoError(t, iter.Err())
	}
	iter.Close()

	count, err = reader.CountRelationships(context.Background(), "another")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Unregister the filter.
	unregisterRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.UnregisterCounter(ctx, "document")
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Try to unregister again.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err = tx.UnregisterCounter(ctx, "document")
		require.Error(t, err)
		require.Contains(t, err.Error(), "counter with name `document` not found")
		return nil
	})

	// Call the filter at the previous revision.
	count, err = reader.CountRelationships(context.Background(), "another")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Call the filter at the unregistered revision.
	reader = ds.SnapshotReader(unregisterRev)
	_, err = reader.CountRelationships(context.Background(), "document")
	require.Error(t, err)
	require.Contains(t, err.Error(), "counter with name `document` not found")
}

func UpdateRelationshipCounterTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(rawDS, require.New(t))

	reader := ds.SnapshotReader(rev)
	filters, err := reader.LookupCounters(context.Background())
	require.NoError(t, err)
	require.Empty(t, filters)

	// Try updating a counter without the filter being registered.
	_, err = ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.StoreCounterValue(ctx, "somedocfilter", 1, rev)
		require.Error(t, err)
		require.Contains(t, err.Error(), "counter with name `somedocfilter` not found")
		return nil
	})
	require.NoError(t, err)

	// Register filter.
	updatedRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "somedocfilter", &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Read the filters.
	reader = ds.SnapshotReader(updatedRev)

	filters, err = reader.LookupCounters(context.Background())
	require.NoError(t, err)
	require.Len(t, filters, 1)

	require.Equal(t, 0, filters[0].Count)
	require.Equal(t, datastore.NoRevision, filters[0].ComputedAtRevision)

	// Update the count for the filter.
	currentRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.StoreCounterValue(ctx, "somedocfilter", 1234, updatedRev)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Read the filters.
	reader = ds.SnapshotReader(currentRev)

	filters, err = reader.LookupCounters(context.Background())
	require.NoError(t, err)

	require.Len(t, filters, 1)
	require.Equal(t, "somedocfilter", filters[0].Name)
	require.Equal(t, 1234, filters[0].Count)
	// we don't use require.Equal, as the internal representation may differ via the optional fields
	// the supported way to compare revisions is via their comparison API methods
	require.True(t, updatedRev.Equal(filters[0].ComputedAtRevision))

	// Register a new filter.
	newFilterRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "another", &core.RelationshipFilter{
			ResourceType: testfixtures.FolderNS.Name,
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Read the filters.
	reader = ds.SnapshotReader(newFilterRev)

	filters, err = reader.LookupCounters(context.Background())
	require.NoError(t, err)
	require.Len(t, filters, 2)
}
