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
func NewCachingDatastoreProxy(delegate datastore.Datastore, c cache.Cache, gcWindow time.Duration, cachingMode CachingMode) datastore.Datastore {
	if c == nil {
		c = cache.NoopCache()
	}

	if cachingMode == JustInTimeCaching {
		log.Info().Type("datastore-type", delegate).Msg("datastore driver explicitly asked to skip schema watch")
		return &definitionCachingProxy{
			Datastore: delegate,
			c:         c,
		}
	}

	// Try to instantiate a schema cache that reads updates from the datastore's schema watch stream. If not possible,
	// fallback to the just-in-time caching proxy.
	unwrapped, ok := delegate.(datastore.UnwrappableDatastore)
	if !ok {
		log.Warn().Type("datastore-type", delegate).Msg("datastore driver does not support unwrapping; falling back to just-in-time caching")
		return &definitionCachingProxy{
			Datastore: delegate,
			c:         c,
		}
	}

	watchable, ok := unwrapped.Unwrap().(datastore.SchemaWatchableDatastore)
	if !ok {
		log.Info().Type("datastore-type", delegate).Msg("datastore driver does not schema watch; falling back to just-in-time caching")
		return &definitionCachingProxy{
			Datastore: delegate,
			c:         c,
		}
	}

	return createWatchingCacheProxy(watchable, c, gcWindow)
}
