package namespace

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/authzed/spicedb/internal/datastore"
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"google.golang.org/protobuf/proto"
)

const (
	errInitialization = "unable to initialize namespace manager: %w"
)

var (
	ErrInvalidNamespace = errors.New("invalid namespace")
	ErrInvalidRelation  = errors.New("invalid relation")
)

type cachingManager struct {
	delegate   datastore.Datastore
	expiration time.Duration
	c          *ristretto.Cache
}

type cacheEntry struct {
	definition *pb.NamespaceDefinition
	version    uint64
	expiration time.Time
}

func NewCachingNamespaceManager(
	delegate datastore.Datastore,
	expiration time.Duration,
	cacheConfig *ristretto.Config,
) (Manager, error) {
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

	return cachingManager{
		delegate:   delegate,
		expiration: expiration,
		c:          cache,
	}, nil
}

func (nsc cachingManager) ReadNamespace(nsName string) (*pb.NamespaceDefinition, uint64, error) {
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
	if err == datastore.ErrNamespaceNotFound {
		return nil, 0, ErrInvalidNamespace
	}
	if err != nil {
		return nil, 0, err
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

func (nsc cachingManager) CheckNamespaceAndRelation(namespace, relation string, allowEllipsis bool) error {
	config, _, err := nsc.ReadNamespace(namespace)
	if err != nil {
		return err
	}

	if allowEllipsis && relation == datastore.Ellipsis {
		return nil
	}

	for _, rel := range config.Relation {
		if rel.Name == relation {
			return nil
		}
	}

	return datastore.ErrRelationNotFound
}
