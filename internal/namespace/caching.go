package namespace

import (
	"context"
	"errors"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/dgraph-io/ristretto"
	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	datastoremw "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/pkg/namespace"
)

const (
	errInitialization = "unable to initialize namespace manager: %w"
)

// nsCache holds the subset of the underlying ristretto.Cache interface that the
// manager needs
type nsCache interface {
	Get(key interface{}) (interface{}, bool)
	Set(key, value interface{}, cost int64) bool
	Close()
}

type cachingManager struct {
	c           nsCache
	readNsGroup singleflight.Group
}

func NewCachingNamespaceManager(
	cacheConfig *ristretto.Config,
) (Manager, error) {
	if cacheConfig == nil {
		cacheConfig = &ristretto.Config{
			NumCounters: 1e4,     // number of keys to track frequency of (10k).
			MaxCost:     1 << 24, // maximum cost of cache (16MB).
			BufferItems: 64,      // number of keys per Get buffer.
		}
	} else {
		log.Info().Int64("numCounters", cacheConfig.NumCounters).Str("maxCost", humanize.Bytes(uint64(cacheConfig.MaxCost))).Msg("configured caching namespace manager")
	}

	cache, err := ristretto.NewCache(cacheConfig)
	if err != nil {
		return nil, fmt.Errorf(errInitialization, err)
	}

	return &cachingManager{
		c: cache,
	}, nil
}

// NewNonCachingNamespaceManager returns a namespace manager that doesn't cache
func NewNonCachingNamespaceManager() Manager {
	return &cachingManager{c: noCache{}}
}

func (nsc *cachingManager) ReadNamespaceAndTypes(ctx context.Context, nsName string, revision decimal.Decimal) (*v0.NamespaceDefinition, *NamespaceTypeSystem, error) {
	nsDef, err := nsc.ReadNamespace(ctx, nsName, revision)
	if err != nil {
		return nsDef, nil, err
	}

	// TODO(jschorr): Cache the type system too
	ts, terr := BuildNamespaceTypeSystemForManager(nsDef, nsc, revision)
	return nsDef, ts, terr
}

func (nsc *cachingManager) ReadNamespace(ctx context.Context, nsName string, revision decimal.Decimal) (*v0.NamespaceDefinition, error) {
	ctx, span := tracer.Start(ctx, "ReadNamespace")
	defer span.End()

	ds := datastoremw.MustFromContext(ctx)

	// Check the nsCache.
	nsRevisionKey, err := ds.NamespaceCacheKey(nsName, revision)
	if err != nil {
		return nil, err
	}
	value, found := nsc.c.Get(nsRevisionKey)
	if found {
		return value.(*v0.NamespaceDefinition), nil
	}

	// We couldn't use the cached entry, load one
	loadedRaw, err, _ := nsc.readNsGroup.Do(nsRevisionKey, func() (interface{}, error) {
		span.AddEvent("Read namespace from delegate (datastore)")
		loaded, _, err := ds.ReadNamespace(ctx, nsName, revision)
		if err != nil {
			return nil, err
		}

		// Remove user-defined metadata.
		namespace.FilterUserDefinedMetadataInPlace(loaded)

		// Save it to the nsCache
		nsc.c.Set(nsRevisionKey, loaded, int64(proto.Size(loaded)))
		span.AddEvent("Saved to nsCache")

		return loaded, err
	})
	if errors.As(err, &datastore.ErrNamespaceNotFound{}) {
		return nil, NewNamespaceNotFoundErr(nsName)
	}
	if err != nil {
		return nil, err
	}

	return loadedRaw.(*v0.NamespaceDefinition), nil
}

func (nsc *cachingManager) CheckNamespaceAndRelation(ctx context.Context, namespace, relation string, allowEllipsis bool, revision decimal.Decimal) error {
	config, err := nsc.ReadNamespace(ctx, namespace, revision)
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

	return NewRelationNotFoundErr(namespace, relation)
}

func (nsc *cachingManager) Close() error {
	nsc.c.Close()
	return nil
}

// noCache is an implementation of nsCache that doesn't cache
type noCache struct{}

var _ nsCache = noCache{}

func (n noCache) Get(key interface{}) (interface{}, bool) {
	return nil, false
}

func (n noCache) Set(key, value interface{}, cost int64) bool {
	return false
}

func (n noCache) Close() {}
