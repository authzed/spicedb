package namespace

import (
	"context"
	"testing"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
)

func TestDisjointCacheKeys(t *testing.T) {
	cache, err := NewCachingNamespaceManager(10*time.Second, nil)
	require.NoError(t, err)

	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC, 0)
	require.NoError(t, err)

	dsA := proxy.NewCacheKeyPrefixProxy(ds, "a")
	dsB := proxy.NewCacheKeyPrefixProxy(ds, "b")

	ctxA := datastoremw.ContextWithDatastore(context.Background(), dsA)
	ctxB := datastoremw.ContextWithDatastore(context.Background(), dsB)

	// write a namespace to the "A" store
	rev, err := dsA.WriteNamespace(ctxA, &v0.NamespaceDefinition{Name: "test/user"})
	require.NoError(t, err)

	def, err := cache.ReadNamespace(ctxA, "test/user", rev)
	require.NoError(t, err)
	require.Equal(t, "test/user", def.Name)

	keyA, err := dsA.NamespaceCacheKey("test/user", rev)
	require.NoError(t, err)

	rCache := cache.(*cachingManager).c
	rCache.Wait()

	nsA, ok := rCache.Get(keyA)
	require.True(t, ok)

	// write a namespace to the "B" store
	revB, err := dsB.WriteNamespace(ctxB, &v0.NamespaceDefinition{Name: "test/user", Relation: []*v0.Relation{{Name: "test"}}})
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
