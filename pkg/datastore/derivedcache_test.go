package datastore_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// keyCounter gives each test a fresh registry key so the tests are safe under `go test -count=N`
// (registration is process-global and panics on duplicate keys by design).
var keyCounter atomic.Int64

func uniqueDerivedCacheKey(prefix string) datastore.DerivedCacheKey {
	return datastore.NewDerivedCacheKey(fmt.Sprintf("%s.%d", prefix, keyCounter.Add(1)))
}

type testCache struct{ id int }

func newStoredSchema() *datastore.ReadOnlyStoredSchema {
	return datastore.NewReadOnlyStoredSchema(&core.StoredSchema{})
}

func TestDerivedCacheLazyAndShared(t *testing.T) {
	key := uniqueDerivedCacheKey("lazy")
	built := 0
	require.NoError(t, datastore.RegisterDerivedCache(key, func() any {
		built++
		return &testCache{id: built}
	}))

	s := newStoredSchema()

	// Built lazily on first access, and the same instance is returned thereafter.
	c1, err := datastore.GetDerivedCache[*testCache](s, key)
	require.NoError(t, err)
	c2, err := datastore.GetDerivedCache[*testCache](s, key)
	require.NoError(t, err)
	require.Same(t, c1, c2)
	require.Equal(t, 1, built, "factory should be invoked exactly once per schema instance")

	// A different stored-schema instance gets its own cache (per-schema-version isolation).
	other := newStoredSchema()
	c3, err := datastore.GetDerivedCache[*testCache](other, key)
	require.NoError(t, err)
	require.NotSame(t, c1, c3)
	require.Equal(t, 2, built)
}

func TestDerivedCacheUnregisteredKeyErrors(t *testing.T) {
	s := newStoredSchema()
	// An unregistered key is a programming error: MustBugf returns a BUG error in production
	// but panics under test, so we assert the panic here.
	require.Panics(t, func() {
		_, _ = datastore.GetDerivedCache[*testCache](s, uniqueDerivedCacheKey("unregistered"))
	})
}

func TestDerivedCacheDuplicateRegistrationErrors(t *testing.T) {
	key := uniqueDerivedCacheKey("dup")
	require.NoError(t, datastore.RegisterDerivedCache(key, func() any { return &testCache{} }))
	require.Error(t, datastore.RegisterDerivedCache(key, func() any { return &testCache{} }))
}
