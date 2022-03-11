package namespace

import (
	"context"
	"testing"

	"github.com/dgraph-io/ristretto"
	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
)

func TestDisjointCacheKeys(t *testing.T) {
	cache, err := NewCachingNamespaceManager(nil)
	require.NoError(t, err)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	dsA := proxy.NewCacheKeyPrefixProxy(ds, "a")
	dsB := proxy.NewCacheKeyPrefixProxy(ds, "b")

	ctxA := datastoremw.ContextWithDatastore(context.Background(), dsA)
	ctxB := datastoremw.ContextWithDatastore(context.Background(), dsB)

	// write a namespace to the "A" store
	rev, err := dsA.WriteNamespace(ctxA, &core.NamespaceDefinition{Name: "test/user"})
	require.NoError(t, err)

	def, err := cache.ReadNamespace(ctxA, "test/user", rev)
	require.NoError(t, err)
	require.Equal(t, "test/user", def.Name)

	keyA, err := dsA.NamespaceCacheKey("test/user", rev)
	require.NoError(t, err)

	rCache := cache.(*cachingManager).c.(*ristretto.Cache)
	rCache.Wait()

	nsA, ok := rCache.Get(keyA)
	require.True(t, ok)

	// write a namespace to the "B" store
	revB, err := dsB.WriteNamespace(ctxB, &core.NamespaceDefinition{Name: "test/user", Relation: []*core.Relation{{Name: "test"}}})
	require.NoError(t, err)

	defB, err := cache.ReadNamespace(ctxB, "test/user", revB)
	require.NoError(t, err)
	require.Equal(t, "test/user", defB.Name)

	keyB, err := dsB.NamespaceCacheKey("test/user", revB)
	require.NoError(t, err)

	rCache.Wait()

	nsB, ok := rCache.Get(keyB)
	require.True(t, ok)

	// namespaces are different
	require.NotEmpty(t, nsA, nsB)
}

func TestNoCache(t *testing.T) {
	cache := NewNonCachingNamespaceManager()
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	ctx := datastoremw.ContextWithDatastore(context.Background(), ds)

	rev, err := ds.WriteNamespace(ctx, &core.NamespaceDefinition{Name: "test/user"})
	require.NoError(t, err)

	def, err := cache.ReadNamespace(ctx, "test/user", rev)
	require.NoError(t, err)
	require.Equal(t, "test/user", def.Name)

	defB, err := cache.ReadNamespace(ctx, "test/user", rev)
	require.NoError(t, err)
	require.Equal(t, "test/user", defB.Name)

	rev, err = ds.WriteNamespace(ctx, &core.NamespaceDefinition{Name: "test/user", Relation: []*core.Relation{{Name: "test"}}})
	require.NoError(t, err)
	defC, err := cache.ReadNamespace(ctx, "test/user", rev)
	require.NoError(t, err)
	require.Equal(t, "test/user", defC.Name)
	require.EqualValues(t, "test", defC.Relation[0].Name)

	require.Equal(t, def, defB)
}
