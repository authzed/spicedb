package namespace

import (
	"context"
	"errors"
	"fmt"
	"time"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/dgraph-io/ristretto"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/proxy"
	"github.com/authzed/spicedb/pkg/namespace"
)

const (
	errInitialization = "unable to initialize namespace manager: %w"
)

type cachingManager struct {
	delegate    datastore.Datastore
	expiration  time.Duration
	c           *ristretto.Cache
	readNsGroup singleflight.Group
}

func cacheKey(nsName string, revision decimal.Decimal) string {
	return fmt.Sprintf("%s@%s", nsName, revision)
}

func NewCachingContextNamespaceManager(expiration time.Duration, cacheConfig *ristretto.Config) (Manager, error) {
	return NewCachingNamespaceManager(proxy.NewContextDatastore(), expiration, cacheConfig)
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

	return &cachingManager{
		delegate:   delegate,
		expiration: expiration,
		c:          cache,
	}, nil
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

	// Check the cache.
	nsRevisionKey := cacheKey(nsName, revision)
	value, found := nsc.c.Get(nsRevisionKey)
	if found {
		return value.(*v0.NamespaceDefinition), nil
	}

	// We couldn't use the cached entry, load one
	loadedRaw, err, _ := nsc.readNsGroup.Do(nsRevisionKey, func() (interface{}, error) {
		span.AddEvent("Read namespace from delegate (datastore)")
		loaded, _, err := nsc.delegate.ReadNamespace(ctx, nsName, revision)
		if err != nil {
			return nil, err
		}

		// Remove user-defined metadata.
		loaded = namespace.FilterUserDefinedMetadata(loaded)

		// Save it to the cache
		nsc.c.Set(cacheKey(nsName, revision), loaded, int64(proto.Size(loaded)))
		span.AddEvent("Saved to cache")

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
