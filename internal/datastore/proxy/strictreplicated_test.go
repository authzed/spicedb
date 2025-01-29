package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revisionparsing"
)

func TestStrictReplicatedReaderWithOnlyPrimary(t *testing.T) {
	primary := fakeDatastore{true, revisionparsing.MustParseRevisionForTest("2")}

	replicated, err := NewStrictReplicatedDatastore(primary)
	require.NoError(t, err)

	require.Equal(t, primary, replicated)
}

func TestStrictReplicatedQueryFallsbackToPrimaryOnRevisionNotAvailableError(t *testing.T) {
	primary := fakeDatastore{true, revisionparsing.MustParseRevisionForTest("2")}
	replica := fakeDatastore{false, revisionparsing.MustParseRevisionForTest("1")}

	replicated, err := NewStrictReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	// Query the replicated, which should fallback to the primary.
	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	iter, err := reader.QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)

	found, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Equal(t, 2, len(found))

	rit, err := reader.ReverseQueryRelationships(context.Background(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	revfound, err := datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Equal(t, 2, len(revfound))

	// Query the replica directly, which should error.
	reader = replica.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	iter, err = reader.QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "revision not available")

	rit, err = reader.ReverseQueryRelationships(context.Background(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(rit)
	require.Error(t, err)
	require.ErrorContains(t, err, "revision not available")

	// Query the replica for a different revision, which should work.
	reader = replica.SnapshotReader(revisionparsing.MustParseRevisionForTest("1"))
	iter, err = reader.QueryRelationships(context.Background(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(found))

	found, err = datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Equal(t, 2, len(found))

	rit, err = reader.ReverseQueryRelationships(context.Background(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	revfound, err = datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Equal(t, 2, len(revfound))
}
