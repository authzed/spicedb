package test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/datastore/queryshape"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func RelationshipCounterOverExpiredTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)

	// Register the filter.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "document", &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Add some expiring and expired relationships.
	updatedRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.WriteRelationships(ctx, []tuple.RelationshipUpdate{
			tuple.Touch(tuple.MustParse("document:somedoc#expiring_viewer@user:tom[expiration:2020-01-01T00:00:00Z]")),
			tuple.Touch(tuple.MustParse("document:somedoc#expiring_viewer@user:fred[expiration:2320-01-01T00:00:00Z]")),
		})
	})
	require.NoError(t, err)

	// Check the count using the filter.
	reader := ds.SnapshotReader(updatedRev)

	expectedCount := 0
	iter, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)

	foundExpiringRel := false
	for rel, err := range iter {
		expectedCount++
		require.NoError(t, err)
		foundExpiringRel = foundExpiringRel || rel.OptionalExpiration != nil
		if rel.OptionalExpiration != nil {
			require.True(t, rel.OptionalExpiration.After(time.Now()))
		}
	}

	require.True(t, foundExpiringRel)

	count, err := reader.CountRelationships(t.Context(), "document")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)
}

func RegisterRelationshipCountersInParallelTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)

	// Run multiple registrations of the counter in parallel and ensure only
	// one succeeds.
	var numSucceeded, numFailed atomic.Int32

	// we retry (memdb.MaxRetries - 1)
	// ideally, we don't have to hardcode this number here and we can do
	// numRetries = ds.MaxRetriesConfigured()
	const numRetries = 9

	failures := make(chan error, numRetries)
	var wg sync.WaitGroup
	for range numRetries {
		wg.Go(func() {
			_, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
				return tx.RegisterCounter(ctx, "document", &core.RelationshipFilter{
					ResourceType: testfixtures.DocumentNS.Name,
				})
			})
			if err != nil {
				failures <- err
				numFailed.Add(1)
			} else {
				numSucceeded.Add(1)
			}
		})
	}

	// Wait for all goroutines to finish.
	wg.Wait()
	close(failures)
	require.Equal(t, int32(1), numSucceeded.Load())
	require.Equal(t, int32(numRetries-1), numFailed.Load())
	for m := range failures {
		require.Contains(t, m.Error(), "counter with name `document` already registered")
	}
}

func RelationshipCountersTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(t, rawDS)

	// Try calling count without the filter being registered.
	reader := ds.SnapshotReader(rev)

	_, err = reader.CountRelationships(t.Context(), "somefilter")
	require.Error(t, err)
	require.Contains(t, err.Error(), "counter with name `somefilter` not found")

	// Register the filter.
	updatedRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
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
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
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
	iter, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)

	for _, err := range iter {
		expectedCount++
		require.NoError(t, err)
	}

	count, err := reader.CountRelationships(t.Context(), "document")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Call another filter.
	expectedCount = 0
	iter, err = reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.FolderNS.Name,
	}, options.WithQueryShape(queryshape.FindResourceOfType))
	require.NoError(t, err)

	for _, err := range iter {
		expectedCount++
		require.NoError(t, err)
	}

	count, err = reader.CountRelationships(t.Context(), "another")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Unregister the filter.
	unregisterRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.UnregisterCounter(ctx, "document")
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Try to unregister again.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err = tx.UnregisterCounter(ctx, "document")
		require.Error(t, err)
		require.Contains(t, err.Error(), "counter with name `document` not found")
		return nil
	})

	// Call the filter at the previous revision.
	count, err = reader.CountRelationships(t.Context(), "another")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)

	// Call the filter at the unregistered revision.
	reader = ds.SnapshotReader(unregisterRev)
	_, err = reader.CountRelationships(t.Context(), "document")
	require.Error(t, err)
	require.Contains(t, err.Error(), "counter with name `document` not found")
}

func RelationshipCountersWithOddFilterTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)
	ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)

	// Register the filter.
	updatedRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "somefilter", &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
			OptionalSubjectFilter: &core.SubjectFilter{
				SubjectType: testfixtures.UserNS.Name,
			},
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Check the count using the filter.
	reader := ds.SnapshotReader(updatedRev)

	expectedCount := 0
	iter, err := reader.QueryRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType: testfixtures.DocumentNS.Name,
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectType: testfixtures.UserNS.Name,
			},
		},
	}, options.WithQueryShape(queryshape.Varying))
	require.NoError(t, err)

	for _, err := range iter {
		expectedCount++
		require.NoError(t, err)
	}

	count, err := reader.CountRelationships(t.Context(), "somefilter")
	require.NoError(t, err)
	require.Equal(t, expectedCount, count)
}

func UpdateRelationshipCounterTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, rev := testfixtures.StandardDatastoreWithData(t, rawDS)

	reader := ds.SnapshotReader(rev)
	filters, err := reader.LookupCounters(t.Context())
	require.NoError(t, err)
	require.Empty(t, filters)

	// Try updating a counter without the filter being registered.
	_, err = ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.StoreCounterValue(ctx, "somedocfilter", 1, rev)
		require.Error(t, err)
		require.Contains(t, err.Error(), "counter with name `somedocfilter` not found")
		return nil
	})
	require.NoError(t, err)

	// Register filter.
	updatedRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "somedocfilter", &core.RelationshipFilter{
			ResourceType: testfixtures.DocumentNS.Name,
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Read the filters.
	reader = ds.SnapshotReader(updatedRev)

	filters, err = reader.LookupCounters(t.Context())
	require.NoError(t, err)
	require.Len(t, filters, 1)

	require.Equal(t, 0, filters[0].Count)
	require.Equal(t, datastore.NoRevision, filters[0].ComputedAtRevision)

	// Update the count for the filter.
	currentRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.StoreCounterValue(ctx, "somedocfilter", 1234, updatedRev)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Read the filters.
	reader = ds.SnapshotReader(currentRev)

	filters, err = reader.LookupCounters(t.Context())
	require.NoError(t, err)

	require.Len(t, filters, 1)
	require.Equal(t, "somedocfilter", filters[0].Name)
	require.Equal(t, 1234, filters[0].Count)
	// we don't use require.Equal, as the internal representation may differ via the optional fields
	// the supported way to compare revisions is via their comparison API methods
	require.True(t, updatedRev.Equal(filters[0].ComputedAtRevision))

	// Register a new filter.
	newFilterRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		err := tx.RegisterCounter(ctx, "another", &core.RelationshipFilter{
			ResourceType: testfixtures.FolderNS.Name,
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Read the filters.
	reader = ds.SnapshotReader(newFilterRev)

	filters, err = reader.LookupCounters(t.Context())
	require.NoError(t, err)
	require.Len(t, filters, 2)
}

// CounterCaseSensitivityTest verifies that relationship counter names are treated as
// case-sensitive: counters whose names differ only in letter case are distinct. Counter
// names are not constrained to lowercase by the API (unlike definition, relation, and caveat
// names), so this is a reachable regression test for datastores whose default collation is
// case-insensitive (e.g. MySQL's utf8mb4_0900_ai_ci), which would otherwise collide
// "casecounter" with "CaseCounter".
func CounterCaseSensitivityTest(t *testing.T, tester DatastoreTester) {
	rawDS, err := tester.New(t, 0, veryLargeGCInterval, veryLargeGCWindow, 1)
	require.NoError(t, err)

	ds, _ := testfixtures.StandardDatastoreWithData(t, rawDS)

	filter := &core.RelationshipFilter{ResourceType: testfixtures.DocumentNS.Name}

	// Register two counters whose names differ only in letter case. Both must be accepted as
	// distinct; on a case-insensitive store the second registration would collide with the
	// first on the (name) unique constraint.
	registeredRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		if err := tx.RegisterCounter(ctx, "casecounter", filter); err != nil {
			return err
		}
		return tx.RegisterCounter(ctx, "CaseCounter", filter)
	})
	require.NoError(t, err)

	// Both distinct counters must be present.
	reader := ds.SnapshotReader(registeredRev)
	counters, err := reader.LookupCounters(t.Context())
	require.NoError(t, err)

	names := make([]string, 0, len(counters))
	for _, c := range counters {
		names = append(names, c.Name)
	}
	require.ElementsMatch(t, []string{"casecounter", "CaseCounter"}, names)

	// Storing a value against one name must not affect the other.
	updatedRev, err := ds.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		return tx.StoreCounterValue(ctx, "casecounter", 1234, registeredRev)
	})
	require.NoError(t, err)

	reader = ds.SnapshotReader(updatedRev)
	counters, err = reader.LookupCounters(t.Context())
	require.NoError(t, err)
	require.Len(t, counters, 2)

	for _, c := range counters {
		switch c.Name {
		case "casecounter":
			require.Equal(t, 1234, c.Count)
		case "CaseCounter":
			require.Equal(t, 0, c.Count)
		default:
			require.Failf(t, "unexpected counter name", "got %q", c.Name)
		}
	}
}
