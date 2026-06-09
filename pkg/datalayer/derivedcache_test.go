package datalayer_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// keyCounter gives each test a fresh registry key so the tests are safe under `go test -count=N`
// (registration is process-global and panics on duplicate keys by design).
var keyCounter atomic.Int64

func uniqueDerivedCacheKey(prefix string) datalayer.DerivedCacheKey {
	return datalayer.NewDerivedCacheKey(fmt.Sprintf("%s.%d", prefix, keyCounter.Add(1)))
}

type testCache struct{ id int }

func newCachedSchema() *datalayer.CachedSchema {
	return datalayer.NewCachedSchema(datastore.NewReadOnlyStoredSchema(&core.StoredSchema{}))
}

func TestDerivedCacheLazyAndShared(t *testing.T) {
	key := uniqueDerivedCacheKey("lazy")
	built := 0
	require.NoError(t, datalayer.RegisterDerivedCache(key, func() any {
		built++
		return &testCache{id: built}
	}))

	cs := newCachedSchema()

	// Built lazily on first access, and the same instance is returned thereafter.
	c1 := datalayer.GetDerivedCache[*testCache](cs, key)
	c2 := datalayer.GetDerivedCache[*testCache](cs, key)
	require.Same(t, c1, c2)
	require.Equal(t, 1, built, "factory should be invoked exactly once per schema instance")

	// A different cached-schema instance gets its own cache (per-schema-version isolation).
	other := newCachedSchema()
	c3 := datalayer.GetDerivedCache[*testCache](other, key)
	require.NotSame(t, c1, c3)
	require.Equal(t, 2, built)
}

func TestDerivedCacheUnregisteredKeyPanics(t *testing.T) {
	cs := newCachedSchema()
	require.Panics(t, func() {
		_ = datalayer.GetDerivedCache[*testCache](cs, uniqueDerivedCacheKey("unregistered"))
	})
}

func TestDerivedCacheDuplicateRegistrationErrors(t *testing.T) {
	key := uniqueDerivedCacheKey("dup")
	require.NoError(t, datalayer.RegisterDerivedCache(key, func() any { return &testCache{} }))
	require.Error(t, datalayer.RegisterDerivedCache(key, func() any { return &testCache{} }))
}
