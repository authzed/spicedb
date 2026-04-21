package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/revisionparsing"
)

// nonStrictDatastore wraps a fakeDatastore but reports strict read mode disabled,
// used to exercise the IsStrictReadModeEnabled==false rejection path.
type nonStrictDatastore struct {
	fakeDatastore
}

func (nonStrictDatastore) IsStrictReadModeEnabled() bool {
	return false
}

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

func TestStrictReplicatedRejectsReplicaWithoutStrictMode(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := nonStrictDatastore{fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("1"), nil}}

	_, err := NewStrictReplicatedDatastore(primary, replica)
	require.Error(t, err)
	require.ErrorContains(t, err, "does not have strict read mode enabled")
}

// TestStrictReplicatedReaderWrapperMethods exercises the legacy caveat/namespace
// wrappers, CountRelationships, and LookupCounters on a strict replicated reader.
// The fake replica returns "not implemented" (not a RevisionUnavailableError), so
// these calls do not trigger a primary fallback; they simply confirm the wrappers
// invoke the replica's reader.
func TestStrictReplicatedReaderWrapperMethods(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("2"), nil}

	replicated, err := NewStrictReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("1"))

	_, _, err = reader.LegacyReadCaveatByName(t.Context(), "is_weekend")
	require.ErrorContains(t, err, "not implemented")

	_, err = reader.LegacyListAllCaveats(t.Context())
	require.ErrorContains(t, err, "not implemented")

	_, err = reader.LegacyLookupCaveatsWithNames(t.Context(), []string{"is_weekend"})
	require.ErrorContains(t, err, "not implemented")

	// LegacyListAllNamespaces returns nil on the fake replica (no error), so happy-path.
	ns, err := reader.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Empty(t, ns)

	_, err = reader.CountRelationships(t.Context(), "filter")
	require.ErrorContains(t, err, "not implemented")

	_, err = reader.LookupCounters(t.Context())
	require.ErrorContains(t, err, "not implemented")
}

// TestStrictReplicatedReaderFallsbackForNamespaceLookups ensures the fallback
// path in LegacyLookupNamespacesWithNames kicks in when the replica returns a
// RevisionUnavailableError. The fake returns that error when queried beyond
// revision 2.
func TestStrictReplicatedReaderFallsbackForNamespaceLookups(t *testing.T) {
	primary := fakeDatastore{"primary", revisionparsing.MustParseRevisionForTest("2"), nil}
	replica := fakeDatastore{"replica", revisionparsing.MustParseRevisionForTest("1"), nil}

	replicated, err := NewStrictReplicatedDatastore(primary, replica)
	require.NoError(t, err)

	reader := replicated.SnapshotReader(revisionparsing.MustParseRevisionForTest("3"))
	ns, err := reader.LegacyLookupNamespacesWithNames(t.Context(), []string{"ns1"})
	require.NoError(t, err)
	require.Len(t, ns, 1)
}
