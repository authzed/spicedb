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

	_, err = reader.CountRelationships(context.Background(), &core.RelationshipFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "the specified filter was not registered")

	// Register the filter.
	updatedRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.NoError(t, err)

		// Try to register again.
		err = tx.RegisterCounter(ctx, &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "the specified filter was already registered")

		// Register another filter.
		err = tx.RegisterCounter(ctx, &core.RelationshipFilter{
			ResourceType: testfixtures.FolderNS.Name,
		})
		require.NoError(t, err)

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

	count, err := reader.CountRelationships(context.Background(), &core.RelationshipFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	})
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

	count, err = reader.CountRelationships(context.Background(), &core.RelationshipFilter{
		ResourceType: testfixtures.FolderNS.Name,
	})
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Unregister the filter.
	unregisterRev, err := ds.ReadWriteTx(context.Background(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.UnregisterCounter(ctx, &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.NoError(t, err)

		// Try to unregister again.
		err = tx.UnregisterCounter(ctx, &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "the specified filter was not registered")

		return nil
	})
	require.NoError(t, err)

	// Call the filter at the previous revision.
	count, err = reader.CountRelationships(context.Background(), &core.RelationshipFilter{
		ResourceType: testfixtures.FolderNS.Name,
	})
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Call the filter at the unregistered revision.
	reader = ds.SnapshotReader(unregisterRev)
	_, err = reader.CountRelationships(context.Background(), &core.RelationshipFilter{
		ResourceType: testfixtures.DocumentNS.Name,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "the specified filter was not registered")
}
