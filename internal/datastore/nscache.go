package datastore

import (
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"google.golang.org/protobuf/proto"
)

const (
	errInitialization = "unable to initialize: %w"
)

type namespaceCache struct {
	delegate   NamespaceManager
	expiration time.Duration
	c          *ristretto.Cache
}

type cacheEntry struct {
	definition *pb.NamespaceDefinition
	version    uint64
	expiration time.Time
}

func NewNamespaceCache(
	delegate NamespaceManager,
	expiration time.Duration,
	cacheConfig *ristretto.Config,
) (NamespaceManager, error) {
	if cacheConfig == nil {
		cacheConfig = &ristretto.Config{
			NumCounters: 1e4,     // number of keys to track frequency of (10k).
			MaxCost:     1 << 24, // maximum cost of cache (16MB).
			BufferItems: 64,      // number of keys per Get buffer.
		}
	}

	cache, err := ristretto.NewCache(cacheConfig)
	if err != nil {
		return nil, fmt.Errorf(errInitialization, err)
	}

	return namespaceCache{
		delegate:   delegate,
		expiration: expiration,
		c:          cache,
	}, nil
}

func (nsc namespaceCache) ReadNamespace(nsName string) (*pb.NamespaceDefinition, uint64, error) {
	// Check the cache.
	now := time.Now()

	value, found := nsc.c.Get(nsName)
	if found {
		foundEntry := value.(cacheEntry)
		if foundEntry.expiration.After(now) {
			return foundEntry.definition, foundEntry.version, nil
		}
	}

	// We couldn't use the cached entry, load one
	loaded, version, err := nsc.delegate.ReadNamespace(nsName)
	if err != nil {
		return loaded, version, err
	}

	// Save it to the cache
	newEntry := cacheEntry{
		definition: loaded,
		version:    version,
		expiration: now.Add(nsc.expiration),
	}
	nsc.c.Set(nsName, newEntry, int64(proto.Size(loaded)))

	return loaded, version, nil
}
