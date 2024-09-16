package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
)

func UseAfterCloseTest(t *testing.T, tester DatastoreTester) {
	require := require.New(t)

	// Create the datastore.
	ds, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(err)

	// Immediately close it.
	err = ds.Close()
	require.NoError(err)

	// Attempt to use and ensure an error is returned.
	_, err = ds.HeadRevision(context.Background())
	require.Error(err)
}

func DeleteAllDataTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, revision := testfixtures.StandardDatastoreWithCaveatedData(rawDS, require.New(t))
	ctx := context.Background()

	// Ensure at least a few relationships and namespaces exist.
	reader := ds.SnapshotReader(revision)
	nsDefs, err := reader.ListAllNamespaces(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, nsDefs, "no namespace definitions provided")

	foundRels := false
	for _, nsDef := range nsDefs {
		iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: nsDef.Definition.Name})
		require.NoError(t, err)

		for range iter {
			foundRels = true
			break
		}
	}
	require.True(t, foundRels, "no relationships provided")

	// Delete all data.
	err = datastore.DeleteAllData(ctx, ds)
	require.NoError(t, err)

	// Ensure there are no relationships or namespaces.
	headRev, err := ds.HeadRevision(ctx)
	require.NoError(t, err)

	reader = ds.SnapshotReader(headRev)
	afterNSDefs, err := reader.ListAllNamespaces(ctx)
	require.NoError(t, err)
	require.Empty(t, afterNSDefs, "namespace definitions still exist")

	for _, nsDef := range nsDefs {
		iter, err := reader.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: nsDef.Definition.Name})
		require.NoError(t, err)

		for range iter {
			require.Fail(t, "relationships still exist")
		}
	}
}
