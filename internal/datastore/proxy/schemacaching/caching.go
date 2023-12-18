package schemacaching

import (
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
)

// CachingMode is the caching mode to use for schema.
type CachingMode int

const (
	// WatchIfSupported will use a schema watch-based cache, if caching is supported. Otherwise,
	// it will fallback to just-in-time caching.
	WatchIfSupported CachingMode = iota

	// JustInTimeCaching will always use a just-in-time cache for schema.
	JustInTimeCaching
)

// DatastoreProxyTestCache returns a cache used for testing.
func DatastoreProxyTestCache(t testing.TB) cache.Cache {
	cache, err := cache.NewCache(&cache.Config{
		NumCounters: 1000,
		MaxCost:     1 * humanize.MiByte,
	})
	require.Nil(t, err)
	return cache
}

// NewCachingDatastoreProxy creates a new datastore proxy which caches definitions that
// are loaded at specific datastore revisions.
func NewCachingDatastoreProxy(delegate datastore.Datastore, c cache.Cache, gcWindow time.Duration, cachingMode CachingMode, watchHeartbeat time.Duration) datastore.Datastore {
	if c == nil {
		c = cache.NoopCache()
	}

	if cachingMode == JustInTimeCaching {
		log.Info().Msg("schema watch explicitly disabled")
		return &definitionCachingProxy{
			Datastore: delegate,
			c:         c,
		}
	}

	return createWatchingCacheProxy(delegate, c, gcWindow, watchHeartbeat)
}
