package schemacaching

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/execscope"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// fullListingReader records backend calls independently of the cache being tested.
type fullListingReader struct {
	datastore.Reader
	namespaces []datastore.RevisionedNamespace
	caveats    []datastore.RevisionedCaveat
	calls      atomic.Int32
	err        error
	gate       <-chan struct{}
}

func (r *fullListingReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	r.calls.Add(1)

	if r.gate != nil {
		select {
		case <-r.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return r.namespaces, r.err
}

func (r *fullListingReader) LegacyListAllCaveats(context.Context) ([]datastore.RevisionedCaveat, error) {
	r.calls.Add(1)
	return r.caveats, r.err
}

func TestFullListingsCacheExactRevisionAndOwnSlice(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(map[bool]string{false: "just-in-time", true: "watch"}[watch], func(t *testing.T) {
			c := DatastoreProxyTestCache(t)
			t.Cleanup(c.Close)
			proxy := &definitionCachingProxy{c: c}
			backend := &fullListingReader{
				namespaces: []datastore.RevisionedNamespace{
					{Definition: &core.NamespaceDefinition{Name: "user"}, LastWrittenRevision: old},
				},
				caveats: []datastore.RevisionedCaveat{
					{Definition: &core.CaveatDefinition{Name: "allowed"}, LastWrittenRevision: zero},
				},
			}

			for _, rev := range []datastore.Revision{one, two} {
				var reader datastore.Reader = &definitionCachingReader{Reader: backend, rev: rev, p: proxy}

				if watch {
					reader = &watchingCachingReader{Reader: backend, rev: rev, p: &watchingCachingProxy{fallbackCache: proxy}}
				}

				for range 2 {
					namespaces, err := reader.LegacyListAllNamespaces(t.Context())
					require.NoError(t, err)
					require.NotNil(t, namespaces[0].Definition)
					require.Equal(t, "user", namespaces[0].Definition.Name)
					require.True(t, old.Equal(namespaces[0].LastWrittenRevision))
					// Replacing an element must not alter the cached listing or backend slice.
					namespaces[0] = datastore.RevisionedNamespace{}

					caveats, err := reader.LegacyListAllCaveats(t.Context())
					require.NoError(t, err)
					require.NotNil(t, caveats[0].Definition)
					require.Equal(t, "allowed", caveats[0].Definition.Name)
					require.True(t, zero.Equal(caveats[0].LastWrittenRevision))
					caveats[0] = datastore.RevisionedCaveat{}
				}
			}

			require.EqualValues(t, 4, backend.calls.Load())
		})
	}
}

func TestFullListingsCacheEmptyAndRetryErrors(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "error"}[failed], func(t *testing.T) {
			c := DatastoreProxyTestCache(t)
			t.Cleanup(c.Close)
			backend := &fullListingReader{}
			reader := &definitionCachingReader{Reader: backend, rev: one, p: &definitionCachingProxy{c: c}}

			if failed {
				backend.err = errors.New("backend unavailable")
				_, err := reader.LegacyListAllNamespaces(t.Context())
				require.ErrorIs(t, err, backend.err)
				backend.err = nil
			}

			for range 2 {
				defs, err := reader.LegacyListAllNamespaces(t.Context())
				require.NoError(t, err)
				require.Empty(t, defs)
			}

			expected := int32(1)

			if failed {
				expected++
			}

			require.Equal(t, expected, backend.calls.Load())
		})
	}
}

func TestFullListingsCoalesceConcurrentMisses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := DatastoreProxyTestCache(t)
		t.Cleanup(c.Close)
		release := make(chan struct{})
		backend := &fullListingReader{gate: release}
		proxy := &definitionCachingProxy{c: c}
		var workers sync.WaitGroup

		for range 32 {
			workers.Go(func() {
				reader := &definitionCachingReader{Reader: backend, rev: revisions.NewForTransactionID(42), p: proxy}
				_, err := reader.LegacyListAllNamespaces(t.Context())
				require.NoError(t, err)
			})
		}

		// All calls are durably blocked in the same outstanding backend load.
		synctest.Wait()
		blockedLoads := backend.calls.Load()
		close(release)
		workers.Wait()
		require.EqualValues(t, 1, blockedLoads)
		require.EqualValues(t, 1, backend.calls.Load())
	})
}

func TestFullListingsHonorSchemaCacheBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := cache.NewStandardCache[cache.StringKey, *cacheEntry](&cache.Config{MaxCost: 1})
		require.NoError(t, err)
		t.Cleanup(c.Close)
		backend := &fullListingReader{namespaces: []datastore.RevisionedNamespace{
			{Definition: &core.NamespaceDefinition{Name: "user"}},
		}}
		reader := &definitionCachingReader{Reader: backend, rev: one, p: &definitionCachingProxy{c: c}}
		_, err = reader.LegacyListAllNamespaces(t.Context())
		require.NoError(t, err)
		// Cache writes are best-effort; wait for asynchronous eviction processing.
		synctest.Wait()
		_, retained := c.Get(cache.StringKey("all:n@" + one.String()))
		require.False(t, retained, "oversized listings must not bypass the configured budget")
	})
}

func TestFullListingsCancellationRegistersBeforeLaunch(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	c := DatastoreProxyTestCache(t)
	t.Cleanup(c.Close)
	backend := &fullListingReader{gate: make(chan struct{})}
	reader := &definitionCachingReader{Reader: backend, rev: one, p: &definitionCachingProxy{c: c}}
	scope := execscope.New()
	ctx, cancel := context.WithCancel(scope.Context(t.Context()))
	finishRoot := execscope.Begin(ctx)
	cancel()
	_, err := reader.LegacyListAllNamespaces(ctx)
	require.ErrorIs(t, err, context.Canceled)
	finishRoot()
	scope.Wait()
	require.EqualValues(t, 1, backend.calls.Load(), "barrier must include the canceled flight before its callback starts")
}

func TestFullListingsPreserveHistoricalSchemaAfterDeletion(t *testing.T) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	c := DatastoreProxyTestCache(t)
	t.Cleanup(c.Close)
	proxy := MustNewDefinitionCachingProxy(ds, c)
	before, err := proxy.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		if err := tx.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "before"}); err != nil {
			return err
		}

		return tx.LegacyWriteCaveats(ctx, []*core.CaveatDefinition{{Name: "old_caveat"}})
	})
	require.NoError(t, err)
	earlier := proxy.SnapshotReader(before)
	defs, err := earlier.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	caveats, err := earlier.LegacyListAllCaveats(t.Context())
	require.NoError(t, err)
	require.Len(t, caveats, 1)

	after, err := proxy.ReadWriteTx(t.Context(), func(ctx context.Context, tx datastore.ReadWriteTransaction) error {
		if err := tx.LegacyDeleteNamespaces(ctx, []string{"before"}, datastore.DeleteNamespacesOnly); err != nil {
			return err
		}

		if err := tx.LegacyDeleteCaveats(ctx, []string{"old_caveat"}); err != nil {
			return err
		}

		return tx.LegacyWriteNamespaces(ctx, &core.NamespaceDefinition{Name: "after"})
	})
	require.NoError(t, err)
	later := proxy.SnapshotReader(after)
	defs, err = later.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Equal(t, "after", defs[0].Definition.Name)
	caveats, err = later.LegacyListAllCaveats(t.Context())
	require.NoError(t, err)
	require.Empty(t, caveats)

	// A cache populated at the newer revision must not replace the historical view.
	defs, err = earlier.LegacyListAllNamespaces(t.Context())
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Equal(t, "before", defs[0].Definition.Name)
	caveats, err = earlier.LegacyListAllCaveats(t.Context())
	require.NoError(t, err)
	require.Len(t, caveats, 1)
	require.Equal(t, "old_caveat", caveats[0].Definition.Name)
}
