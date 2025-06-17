package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revisionparsing"
)

func TestStrictReplicatedReaderWithOnlyPrimary(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}

	replicated, err := NewStrictReplicatedDatastore(primary)
	require.NoError(t, err)

	require.Equal(t, primary, replicated)
}

func TestStrictReplicatedQueryFallsbackToPrimaryOnRevisionNotAvailableError(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("1"), nil}

	replicated, err := NewStrictReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	// Query the replicated, which should fallback to the primary.
	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	iter, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)

	found, err := datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Len(t, found, 2)

	rit, err := reader.ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	revfound, err := datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Len(t, revfound, 2)

	// Query the replica directly, which should error.
	reader = replica.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	iter, err = reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(iter)
	require.Error(t, err)
	require.ErrorContains(t, err, "revision not available")

	rit, err = reader.ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	_, err = datastore.IteratorToSlice(rit)
	require.Error(t, err)
	require.ErrorContains(t, err, "revision not available")

	// Query the replica for a different revision, which should work.
	reader = replica.SnapshotReader(revisionparsing.MustParseRevisionForTest("1"))
	iter, err = reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.NoError(t, err)
	require.Len(t, found, 2)

	found, err = datastore.IteratorToSlice(iter)
	require.NoError(t, err)
	require.Len(t, found, 2)

	rit, err = reader.ReverseQueryRelationships(t.Context(), datastore.SubjectsFilter{
		SubjectType: "user",
	})
	require.NoError(t, err)

	revfound, err = datastore.IteratorToSlice(rit)
	require.NoError(t, err)
	require.Len(t, revfound, 2)
}

func TestStrictReplicatedQueryNonFallbackError(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica-with-normal-error", revisionparsing.MustParseRevisionForTest("1"), nil}

	replicated, err := NewStrictReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	// Query the replicated, which should return the error.
	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	_, err = reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: "resource",
	})
	require.ErrorContains(t, err, "raising an expected error")
}
