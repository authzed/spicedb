package schemacaching

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/testutil"
)

func TestOldWatchingCacheBasicOperation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	wcache := createWatchingCacheProxy(fakeDS, cache.NoopCache[cache.StringKey, *cacheEntry](), 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Ensure no namespaces are found.
	_, _, err := wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})
	require.False(t, wcache.namespaceCache.inFallbackMode)

	// Ensure a re-read also returns not found, even before a checkpoint is received.
	_, _, err = wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})

	// Send a checkpoint for revision 1.
	fakeDS.sendCheckpoint(rev("1"))

	// Write a namespace update at revision 2.
	fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev("2"))

	// Ensure that reading at rev 2 returns found.
	nsDef, _, err := wcache.SnapshotReader(rev("2")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Disable reads.
	fakeDS.disableReads()

	// Ensure that reading at rev 3 returns an error, as with reads disabled the cache should not be hit.
	_, _, err = wcache.SnapshotReader(rev("3")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.Error(t, err)
	require.ErrorContains(t, err, "reads are disabled")

	// Re-enable reads.
	fakeDS.enableReads()

	// Ensure that reading at rev 3 returns found, even though the cache should not yet be there. This will
	// require a datastore fallback read because the cache is not yet checkedpointed to that revision.
	nsDef, _, err = wcache.SnapshotReader(rev("3")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Checkpoint to rev 4.
	fakeDS.sendCheckpoint(rev("4"))
	require.False(t, wcache.namespaceCache.inFallbackMode)

	// Disable reads.
	fakeDS.disableReads()

	// Read again, which should now be via the cache.
	nsDef, _, err = wcache.SnapshotReader(rev("3.0000000005")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Read via a lookup.
	nsDefs, err := wcache.SnapshotReader(rev("3.0000000005")).LegacyLookupNamespacesWithNames(t.Context(), []string{"somenamespace"})
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDefs[0].Definition.Name)

	// Delete the namespace at revision 5.
	fakeDS.updateNamespace("somenamespace", nil, rev("5"))

	// Re-read at an earlier revision.
	nsDef, _, err = wcache.SnapshotReader(rev("3.0000000005")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Read at revision 5.
	_, _, err = wcache.SnapshotReader(rev("5")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.Error(t, err)
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{}, "missing not found in: %v", err)

	// Lookup at revision 5.
	nsDefs, err = wcache.SnapshotReader(rev("5")).LegacyLookupNamespacesWithNames(t.Context(), []string{"somenamespace"})
	require.NoError(t, err)
	require.Empty(t, nsDefs)

	// Update a caveat.
	fakeDS.updateCaveat("somecaveat", &corev1.CaveatDefinition{Name: "somecaveat"}, rev("6"))

	// Read at revision 6.
	caveatDef, _, err := wcache.SnapshotReader(rev("6")).LegacyReadCaveatByName(t.Context(), "somecaveat")
	require.NoError(t, err)
	require.Equal(t, "somecaveat", caveatDef.Name)

	// Attempt to read at revision 1, which should require a read.
	_, _, err = wcache.SnapshotReader(rev("1")).LegacyReadCaveatByName(t.Context(), "somecaveat")
	require.ErrorContains(t, err, "reads are disabled")
}

func TestWatchingCacheBasicOperation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	wcache := createWatchingCacheProxy(fakeDS, cache.NoopCache[cache.StringKey, *cacheEntry](), 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Ensure no namespaces are found.
	_, _, err := wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})
	require.False(t, wcache.namespaceCache.inFallbackMode)

	// Ensure a re-read also returns not found, even before a checkpoint is received.
	_, _, err = wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})

	// Send a checkpoint for revision 1.
	fakeDS.sendCheckpoint(rev("1"))

	// Write a namespace update at revision 2.
	fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev("2"))

	// Ensure that reading at rev 2 returns found.
	nsDef, _, err := wcache.SnapshotReader(rev("2")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Disable reads.
	fakeDS.disableReads()

	// Ensure that reading at rev 3 returns an error, as with reads disabled the cache should not be hit.
	_, _, err = wcache.SnapshotReader(rev("3")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.Error(t, err)
	require.ErrorContains(t, err, "reads are disabled")

	// Re-enable reads.
	fakeDS.enableReads()

	// Ensure that reading at rev 3 returns found, even though the cache should not yet be there. This will
	// require a datastore fallback read because the cache is not yet checkedpointed to that revision.
	nsDef, _, err = wcache.SnapshotReader(rev("3")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Checkpoint to rev 4.
	fakeDS.sendCheckpoint(rev("4"))
	require.False(t, wcache.namespaceCache.inFallbackMode)

	// Disable reads.
	fakeDS.disableReads()

	// Read again, which should now be via the cache.
	nsDef, _, err = wcache.SnapshotReader(rev("3.0000000005")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Read via a lookup.
	nsDefs, err := wcache.SnapshotReader(rev("3.0000000005")).LegacyLookupNamespacesWithNames(t.Context(), []string{"somenamespace"})
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDefs[0].Definition.Name)

	// Delete the namespace at revision 5.
	fakeDS.updateNamespace("somenamespace", nil, rev("5"))

	// Re-read at an earlier revision.
	nsDef, _, err = wcache.SnapshotReader(rev("3.0000000005")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", nsDef.Name)

	// Read at revision 5.
	_, _, err = wcache.SnapshotReader(rev("5")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.Error(t, err)
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{}, "missing not found in: %v", err)

	// Lookup at revision 5.
	nsDefs, err = wcache.SnapshotReader(rev("5")).LegacyLookupNamespacesWithNames(t.Context(), []string{"somenamespace"})
	require.NoError(t, err)
	require.Empty(t, nsDefs)

	// Update a caveat.
	fakeDS.updateCaveat("somecaveat", &corev1.CaveatDefinition{Name: "somecaveat"}, rev("6"))

	// Read at revision 6.
	caveatDef, _, err := wcache.SnapshotReader(rev("6")).LegacyReadCaveatByName(t.Context(), "somecaveat")
	require.NoError(t, err)
	require.Equal(t, "somecaveat", caveatDef.Name)

	// Attempt to read at revision 1, which should require a read.
	_, _, err = wcache.SnapshotReader(rev("1")).LegacyReadCaveatByName(t.Context(), "somecaveat")
	require.ErrorContains(t, err, "reads are disabled")
}

func TestOldWatchingCacheParallelOperations(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	wcache := createWatchingCacheProxy(fakeDS, cache.NoopCache[cache.StringKey, *cacheEntry](), 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Run some operations in parallel.
	var wg sync.WaitGroup
	wg.Add(2)

	firstErrs := make(chan error, 2)
	firstFallbackModes := make(chan bool, 1)
	firstNsDefNames := make(chan string, 1)

	secondErrs := make(chan error, 2)
	secondFallbackModes := make(chan bool, 2)

	go (func() {
		defer wg.Done()

		// Read somenamespace (which should not be found)
		_, _, err := wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
		firstErrs <- err
		firstFallbackModes <- wcache.namespaceCache.inFallbackMode

		// Write somenamespace.
		fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev("2"))

		// Read again (which should be found now)
		nsDef, _, err := wcache.SnapshotReader(rev("2")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
		firstErrs <- err
		firstNsDefNames <- nsDef.Name
	})()

	go (func() {
		defer wg.Done()

		// Read anothernamespace (which should not be found)
		_, _, err := wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "anothernamespace")
		secondErrs <- err
		secondFallbackModes <- wcache.namespaceCache.inFallbackMode

		// Read again (which should still not be found)
		_, _, err = wcache.SnapshotReader(rev("3")).LegacyReadNamespaceByName(t.Context(), "anothernamespace")
		secondErrs <- err
		secondFallbackModes <- wcache.namespaceCache.inFallbackMode
	})()

	wg.Wait()

	var nsNotFoundErr datastore.NamespaceNotFoundError

	// Make the assertions
	// Assertions on first goroutine
	// Non-existent namespace
	err := <-firstErrs
	require.ErrorAs(t, err, &nsNotFoundErr)
	inFallbackMode := <-firstFallbackModes
	require.False(t, inFallbackMode)

	// Namespace that we expect to exist
	err = <-firstErrs
	require.NoError(t, err, "expected namespace read from rev 2 to succeed")
	name := <-firstNsDefNames
	require.Equal(t, "somenamespace", name)

	// Assertions on second goroutine
	// Reading a non-existent namespace
	err = <-secondErrs
	require.ErrorAs(t, err, &nsNotFoundErr)
	inFallbackMode = <-secondFallbackModes
	require.False(t, inFallbackMode)

	// Reading another non-existent namespace
	err = <-secondErrs
	require.ErrorAs(t, err, &nsNotFoundErr)
	inFallbackMode = <-secondFallbackModes
	require.False(t, inFallbackMode)
}

func TestWatchingCacheParallelOperations(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	wcache := createWatchingCacheProxy(fakeDS, cache.NoopCache[cache.StringKey, *cacheEntry](), 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Run some operations in parallel.
	var wg sync.WaitGroup
	wg.Add(2)

	firstErrs := make(chan error, 2)
	firstFallbackModes := make(chan bool, 1)
	firstNsDefNames := make(chan string, 1)

	secondErrs := make(chan error, 2)
	secondFallbackModes := make(chan bool, 2)

	go (func() {
		defer wg.Done()

		// Read somenamespace (which should not be found)
		_, _, err := wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
		firstErrs <- err
		firstFallbackModes <- wcache.namespaceCache.inFallbackMode

		// Write somenamespace.
		fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev("2"))

		// Read again (which should be found now)
		nsDef, _, err := wcache.SnapshotReader(rev("2")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
		firstErrs <- err
		firstNsDefNames <- nsDef.Name
	})()

	go (func() {
		defer wg.Done()

		// Read anothernamespace (which should not be found)
		_, _, err := wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "anothernamespace")
		secondErrs <- err
		secondFallbackModes <- wcache.namespaceCache.inFallbackMode

		// Read again (which should still not be found)
		_, _, err = wcache.SnapshotReader(rev("3")).LegacyReadNamespaceByName(t.Context(), "anothernamespace")
		secondErrs <- err
		secondFallbackModes <- wcache.namespaceCache.inFallbackMode
	})()

	wg.Wait()

	var nsNotFoundErr datastore.NamespaceNotFoundError

	// Make the assertions
	// Assertions on first goroutine
	// Non-existent namespace
	err := <-firstErrs
	require.ErrorAs(t, err, &nsNotFoundErr)
	inFallbackMode := <-firstFallbackModes
	require.False(t, inFallbackMode)

	// Namespace that we expect to exist
	err = <-firstErrs
	require.NoError(t, err, "expected namespace read from rev 2 to succeed")
	name := <-firstNsDefNames
	require.Equal(t, "somenamespace", name)

	// Assertions on second goroutine
	// Reading a non-existent namespace
	err = <-secondErrs
	require.ErrorAs(t, err, &nsNotFoundErr)
	inFallbackMode = <-secondFallbackModes
	require.False(t, inFallbackMode)

	// Reading another non-existent namespace
	err = <-secondErrs
	require.ErrorAs(t, err, &nsNotFoundErr)
	inFallbackMode = <-secondFallbackModes
	require.False(t, inFallbackMode)
}

func TestWatchingCacheParallelReaderWriter(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	wcache := createWatchingCacheProxy(fakeDS, cache.NoopCache[cache.StringKey, *cacheEntry](), 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Write somenamespace.
	fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev("0"))

	// Run some operations in parallel.
	var wg sync.WaitGroup
	wg.Add(2)

	go (func() {
		// Start a loop to write a namespace a bunch of times.
		for i := range 1000 {
			// Write somenamespace.
			fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev(fmt.Sprintf("%d", i+1)))
		}

		wg.Done()
	})()

	headRevisionErrors := make(chan error, 1000)
	snapshotReaderErrors := make(chan error, 1000)
	namespaceNames := make(chan string, 1000)

	go (func() {
		// Start a loop to read a namespace a bunch of times.
		for range 1000 {
			headRevision, err := fakeDS.HeadRevision(t.Context())
			headRevisionErrors <- err

			nsDef, _, err := wcache.SnapshotReader(headRevision).LegacyReadNamespaceByName(t.Context(), "somenamespace")
			snapshotReaderErrors <- err
			namespaceNames <- nsDef.Name
		}

		wg.Done()
	})()

	wg.Wait()

	// 1000 iterations, 3 channels
	for range 3000 {
		select {
		case headRevisionErr := <-headRevisionErrors:
			require.NoError(t, headRevisionErr, "unexpected error getting head revision")
		case snapshotReaderErr := <-snapshotReaderErrors:
			require.NoError(t, snapshotReaderErr, "unexpected error reading namespace")
		case namespaceName := <-namespaceNames:
			require.Equal(t, "somenamespace", namespaceName)
		}
	}
}

func TestOldWatchingCacheParallelReaderWriter(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	wcache := createWatchingCacheProxy(fakeDS, cache.NoopCache[cache.StringKey, *cacheEntry](), 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Write somenamespace.
	fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev("0"))

	// Run some operations in parallel.
	var wg sync.WaitGroup
	wg.Add(2)

	go (func() {
		// Start a loop to write a namespace a bunch of times.
		for i := range 1000 {
			// Write somenamespace.
			fakeDS.updateNamespace("somenamespace", &corev1.NamespaceDefinition{Name: "somenamespace"}, rev(fmt.Sprintf("%d", i+1)))
		}

		wg.Done()
	})()

	headRevisionErrors := make(chan error, 1000)
	snapshotReaderErrors := make(chan error, 1000)
	namespaceNames := make(chan string, 1000)

	go (func() {
		// Start a loop to read a namespace a bunch of times.
		for range 1000 {
			headRevision, err := fakeDS.HeadRevision(t.Context())
			headRevisionErrors <- err

			nsDef, _, err := wcache.SnapshotReader(headRevision).LegacyReadNamespaceByName(t.Context(), "somenamespace")
			snapshotReaderErrors <- err
			namespaceNames <- nsDef.Name
		}

		wg.Done()
	})()

	wg.Wait()

	// 1000 iterations, 3 channels
	for range 3000 {
		select {
		case headRevisionErr := <-headRevisionErrors:
			require.NoError(t, headRevisionErr, "unexpected error getting head revision")
		case snapshotReaderErr := <-snapshotReaderErrors:
			require.NoError(t, snapshotReaderErr, "unexpected error reading namespace")
		case namespaceName := <-namespaceNames:
			require.Equal(t, "somenamespace", namespaceName)
		}
	}
}

func TestWatchingCacheFallbackToStandardCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	c, err := cache.NewStandardCache[cache.StringKey, *cacheEntry](&cache.Config{
		NumCounters: 1000,
		MaxCost:     10000,
		DefaultTTL:  10000 * time.Second,
	})
	require.NoError(t, err)

	wcache := createWatchingCacheProxy(fakeDS, c, 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Ensure the namespace is not found, but is cached in the fallback caching layer.
	r := rev("1")
	_, _, err = wcache.SnapshotReader(r).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})
	require.False(t, wcache.namespaceCache.inFallbackMode)

	expectedKey := cache.StringKey("n:somenamespace@" + r.String())
	entry, ok := c.Get(expectedKey)
	require.True(t, ok)
	require.Error(t, entry.notFound)

	// Disable reading and ensure it still works, via the fallback cache.
	fakeDS.readsDisabled = true

	_, _, err = wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})
	require.False(t, wcache.namespaceCache.inFallbackMode)
}

func TestOldWatchingCacheFallbackToStandardCache(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("0"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
	}

	c, err := cache.NewStandardCache[cache.StringKey, *cacheEntry](&cache.Config{
		NumCounters: 1000,
		MaxCost:     10000,
		DefaultTTL:  10000 * time.Second,
	})
	require.NoError(t, err)

	wcache := createWatchingCacheProxy(fakeDS, c, 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Ensure the namespace is not found, but is cached in the fallback caching layer.
	r := rev("1")
	_, _, err = wcache.SnapshotReader(r).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})
	require.False(t, wcache.namespaceCache.inFallbackMode)

	expectedKey := cache.StringKey("n:somenamespace@" + r.String())
	entry, ok := c.Get(expectedKey)
	require.True(t, ok)
	require.Error(t, entry.notFound)

	// Disable reading and ensure it still works, via the fallback cache.
	fakeDS.readsDisabled = true

	_, _, err = wcache.SnapshotReader(rev("1")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.ErrorAs(t, err, &datastore.NamespaceNotFoundError{})
	require.False(t, wcache.namespaceCache.inFallbackMode)
}

func TestOldWatchingCachePrepopulated(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("4"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
		existingNamespaces: []datastore.RevisionedNamespace{
			datastore.RevisionedDefinition[*corev1.NamespaceDefinition]{
				Definition: &corev1.NamespaceDefinition{
					Name: "somenamespace",
				},
				LastWrittenRevision: rev("1"),
			},
			datastore.RevisionedDefinition[*corev1.NamespaceDefinition]{
				Definition: &corev1.NamespaceDefinition{
					Name: "anothernamespace",
				},
				LastWrittenRevision: rev("2"),
			},
		},
	}

	c, err := cache.NewStandardCache[cache.StringKey, *cacheEntry](&cache.Config{
		NumCounters: 1000,
		MaxCost:     1000,
		DefaultTTL:  1000 * time.Second,
	})
	require.NoError(t, err)

	wcache := createWatchingCacheProxy(fakeDS, c, 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Ensure the namespace is found.
	def, _, err := wcache.SnapshotReader(rev("4")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", def.Name)
}

func TestWatchingCachePrepopulated(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t, testutil.GoLeakIgnores()...)
	})

	fakeDS := &fakeDatastore{
		headRevision: rev("4"),
		namespaces:   map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition]{},
		caveats:      map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]{},
		schemaChan:   make(chan datastore.RevisionChanges, 1),
		errChan:      make(chan error, 1),
		existingNamespaces: []datastore.RevisionedNamespace{
			datastore.RevisionedDefinition[*corev1.NamespaceDefinition]{
				Definition: &corev1.NamespaceDefinition{
					Name: "somenamespace",
				},
				LastWrittenRevision: rev("1"),
			},
			datastore.RevisionedDefinition[*corev1.NamespaceDefinition]{
				Definition: &corev1.NamespaceDefinition{
					Name: "anothernamespace",
				},
				LastWrittenRevision: rev("2"),
			},
		},
	}

	c, err := cache.NewStandardCache[cache.StringKey, *cacheEntry](&cache.Config{
		NumCounters: 1000,
		MaxCost:     1000,
		DefaultTTL:  1000 * time.Second,
	})
	require.NoError(t, err)

	wcache := createWatchingCacheProxy(fakeDS, c, 1*time.Hour, 100*time.Millisecond)
	require.NoError(t, wcache.startSync(t.Context()))
	t.Cleanup(func() {
		wcache.Close()
	})

	// Ensure the namespace is found.
	def, _, err := wcache.SnapshotReader(rev("4")).LegacyReadNamespaceByName(t.Context(), "somenamespace")
	require.NoError(t, err)
	require.Equal(t, "somenamespace", def.Name)
}

type fakeDatastore struct {
	lock sync.RWMutex

	readsDisabled bool                                                                               // GUARDED_BY(lock)
	headRevision  datastore.Revision                                                                 // GUARDED_BY(lock)
	namespaces    map[string][]fakeEntry[datastore.RevisionedNamespace, *corev1.NamespaceDefinition] // GUARDED_BY(lock)
	caveats       map[string][]fakeEntry[datastore.RevisionedCaveat, *corev1.CaveatDefinition]       // GUARDED_BY(lock)

	schemaChan chan datastore.RevisionChanges
	errChan    chan error

	existingNamespaces []datastore.RevisionedNamespace
}

func (fds *fakeDatastore) MetricsID() (string, error) {
	return "fake", nil
}

func (fds *fakeDatastore) UniqueID(_ context.Context) (string, error) {
	return "fakedsforwatch", nil
}

func (fds *fakeDatastore) updateNamespace(name string, def *corev1.NamespaceDefinition, revision datastore.Revision) {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	updateDef(fds.namespaces, name, def, def == nil, revision, fds.schemaChan)
	fds.headRevision = revision
}

func (fds *fakeDatastore) updateCaveat(name string, def *corev1.CaveatDefinition, revision datastore.Revision) {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	updateDef(fds.caveats, name, def, def == nil, revision, fds.schemaChan)
	fds.headRevision = revision
}

func (fds *fakeDatastore) sendCheckpoint(revision datastore.Revision) {
	fds.schemaChan <- datastore.RevisionChanges{
		Revision:     revision,
		IsCheckpoint: true,
	}
	time.Sleep(1 * time.Millisecond)
}

type fakeEntry[T datastore.RevisionedDefinition[Q], Q datastore.SchemaDefinition] struct {
	value      T
	wasDeleted bool
}

type revisionGetter[T datastore.SchemaDefinition] interface {
	datastore.RevisionedDefinition[T]
	GetLastWrittenRevision() datastore.Revision
}

func updateDef[T datastore.SchemaDefinition](
	defs map[string][]fakeEntry[datastore.RevisionedDefinition[T], T],
	name string,
	def T,
	isDelete bool,
	revision datastore.Revision,
	schemaChan chan datastore.RevisionChanges,
) {
	slice, ok := defs[name]
	if !ok {
		slice = []fakeEntry[datastore.RevisionedDefinition[T], T]{}
	}

	slice = append(slice, fakeEntry[datastore.RevisionedDefinition[T], T]{
		value: datastore.RevisionedDefinition[T]{
			Definition:          def,
			LastWrittenRevision: revision,
		},
		wasDeleted: isDelete,
	})
	defs[name] = slice

	if isDelete {
		schemaChan <- datastore.RevisionChanges{
			Revision:          revision,
			DeletedNamespaces: []string{name},
		}
	} else {
		schemaChan <- datastore.RevisionChanges{
			Revision:           revision,
			ChangedDefinitions: []datastore.SchemaDefinition{def},
		}
	}
	time.Sleep(1 * time.Millisecond)
}

func readDefs[T datastore.SchemaDefinition, Q revisionGetter[T]](defs map[string][]fakeEntry[Q, T], names []string, revision datastore.Revision) []Q {
	results := make([]Q, 0, len(names))
	for _, name := range names {
		revisionedDefs, ok := defs[name]
		if !ok {
			continue
		}

		revisioned := []fakeEntry[Q, T]{}
		for _, revisionedEntry := range revisionedDefs {
			if revisionedEntry.value.GetLastWrittenRevision().LessThan(revision) || revisionedEntry.value.GetLastWrittenRevision().Equal(revision) {
				revisioned = append(revisioned, revisionedEntry)
			}
		}

		if len(revisioned) == 0 {
			continue
		}

		slices.SortFunc(revisioned, func(a fakeEntry[Q, T], b fakeEntry[Q, T]) int {
			if a.value.GetLastWrittenRevision().Equal(b.value.GetLastWrittenRevision()) {
				return 0
			}

			if a.value.GetLastWrittenRevision().LessThan(b.value.GetLastWrittenRevision()) {
				return -1
			}

			return 1
		})

		entry := revisioned[len(revisioned)-1]
		if !entry.wasDeleted {
			results = append(results, entry.value)
		}
	}

	return results
}

func (fds *fakeDatastore) readNamespaces(names []string, revision datastore.Revision) ([]datastore.RevisionedNamespace, error) {
	fds.lock.RLock()
	defer fds.lock.RUnlock()

	if fds.readsDisabled {
		return nil, fmt.Errorf("reads are disabled")
	}

	return readDefs(fds.namespaces, names, revision), nil
}

func (fds *fakeDatastore) readCaveats(names []string, revision datastore.Revision) ([]datastore.RevisionedCaveat, error) {
	fds.lock.RLock()
	defer fds.lock.RUnlock()

	if fds.readsDisabled {
		return nil, fmt.Errorf("reads are disabled")
	}

	return readDefs(fds.caveats, names, revision), nil
}

func (fds *fakeDatastore) disableReads() {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	fds.readsDisabled = true
}

func (fds *fakeDatastore) enableReads() {
	fds.lock.Lock()
	defer fds.lock.Unlock()

	fds.readsDisabled = false
}

func (fds *fakeDatastore) SnapshotReader(rev datastore.Revision) datastore.Reader {
	return &fakeSnapshotReader{fds, rev}
}

func (fds *fakeDatastore) HeadRevision(context.Context) (datastore.Revision, error) {
	fds.lock.RLock()
	defer fds.lock.RUnlock()

	return fds.headRevision, nil
}

func (*fakeDatastore) ReadWriteTx(context.Context, datastore.TxUserFunc, ...options.RWTOptionsOption) (datastore.Revision, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*fakeDatastore) CheckRevision(context.Context, datastore.Revision) error {
	return nil
}

func (*fakeDatastore) Close() error {
	return nil
}

func (*fakeDatastore) Features(context.Context) (*datastore.Features, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*fakeDatastore) OfflineFeatures() (*datastore.Features, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*fakeDatastore) OptimizedRevision(context.Context) (datastore.Revision, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*fakeDatastore) ReadyState(context.Context) (datastore.ReadyState, error) {
	return datastore.ReadyState{}, fmt.Errorf("not implemented")
}

func (*fakeDatastore) RevisionFromString(string) (datastore.Revision, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*fakeDatastore) Statistics(context.Context) (datastore.Stats, error) {
	return datastore.Stats{}, fmt.Errorf("not implemented")
}

func (fds *fakeDatastore) Watch(_ context.Context, _ datastore.Revision, opts datastore.WatchOptions) (<-chan datastore.RevisionChanges, <-chan error) {
	if opts.Content&datastore.WatchSchema != datastore.WatchSchema {
		panic("unexpected option")
	}

	return fds.schemaChan, fds.errChan
}

type fakeSnapshotReader struct {
	fds *fakeDatastore
	rev datastore.Revision
}

func (fsr *fakeSnapshotReader) CountRelationships(ctx context.Context, name string) (int, error) {
	return -1, fmt.Errorf("not implemented")
}

func (fsr *fakeSnapshotReader) LookupCounters(ctx context.Context) ([]datastore.RelationshipCounter, error) {
	return nil, fmt.Errorf("not implemented")
}

func (fsr *fakeSnapshotReader) LegacyLookupNamespacesWithNames(_ context.Context, nsNames []string) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	return fsr.fds.readNamespaces(nsNames, fsr.rev)
}

func (fsr *fakeSnapshotReader) LegacyReadNamespaceByName(_ context.Context, nsName string) (ns *corev1.NamespaceDefinition, lastWritten datastore.Revision, err error) {
	namespaces, err := fsr.fds.readNamespaces([]string{nsName}, fsr.rev)
	if err != nil {
		return nil, nil, err
	}

	if len(namespaces) == 0 {
		return nil, nil, datastore.NewNamespaceNotFoundErr(nsName)
	}
	return namespaces[0].Definition, namespaces[0].LastWrittenRevision, nil
}

func (fsr *fakeSnapshotReader) LegacyLookupCaveatsWithNames(_ context.Context, names []string) ([]datastore.RevisionedDefinition[*corev1.CaveatDefinition], error) {
	return fsr.fds.readCaveats(names, fsr.rev)
}

func (fsr *fakeSnapshotReader) LegacyReadCaveatByName(_ context.Context, name string) (caveat *corev1.CaveatDefinition, lastWritten datastore.Revision, err error) {
	caveats, err := fsr.fds.readCaveats([]string{name}, fsr.rev)
	if err != nil {
		return nil, nil, err
	}

	if len(caveats) == 0 {
		return nil, nil, datastore.NewCaveatNameNotFoundErr(name)
	}
	return caveats[0].Definition, caveats[0].LastWrittenRevision, nil
}

func (*fakeSnapshotReader) LegacyListAllCaveats(context.Context) ([]datastore.RevisionedDefinition[*corev1.CaveatDefinition], error) {
	return []datastore.RevisionedDefinition[*corev1.CaveatDefinition]{}, nil
}

func (fsr *fakeSnapshotReader) LegacyListAllNamespaces(context.Context) ([]datastore.RevisionedDefinition[*corev1.NamespaceDefinition], error) {
	if fsr.fds.existingNamespaces != nil {
		return fsr.fds.existingNamespaces, nil
	}

	return []datastore.RevisionedDefinition[*corev1.NamespaceDefinition]{}, nil
}

func (*fakeSnapshotReader) QueryRelationships(context.Context, datastore.RelationshipsFilter, ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	return nil, fmt.Errorf("not implemented")
}

func (*fakeSnapshotReader) ReverseQueryRelationships(context.Context, datastore.SubjectsFilter, ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	return nil, fmt.Errorf("not implemented")
}
