package proxy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/singleflight"

	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// namespaceDefinitionSizeVTMultiplier is the mulitiplier to be used for
// estimating the in-memory cost of a NamespaceDefinition based on its
// on-wire size, as returned by SizeVT. This was determined by testing
// all existing namespace definitions found in consistency tests and is
// enforced via the estimatednssize_test.
const (
	namespaceDefinitionSizeVTMultiplier = 10
	namespaceDefinitionMinimumSize      = 150
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

// NewCachingDatastoreProxy creates a new datastore proxy which caches namespace definitions that
// are loaded at specific datastore revisions.
func NewCachingDatastoreProxy(delegate datastore.Datastore, c cache.Cache) datastore.Datastore {
	if c == nil {
		c = cache.NoopCache()
	}
	return &nsCachingProxy{
		Datastore: delegate,
		c:         c,
	}
}

type nsCachingProxy struct {
	datastore.Datastore
	c           cache.Cache
	readNsGroup singleflight.Group
}

func (p *nsCachingProxy) Close() error {
	p.c.Close()
	return p.Datastore.Close()
}

func (p *nsCachingProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.Datastore.SnapshotReader(rev)
	return &nsCachingReader{delegateReader, rev, p}
}

func (p *nsCachingProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
) (datastore.Revision, error) {
	return p.Datastore.ReadWriteTx(ctx, func(delegateRWT datastore.ReadWriteTransaction) error {
		rwt := &nsCachingRWT{delegateRWT, &sync.Map{}}
		return f(rwt)
	})
}

type nsCachingReader struct {
	datastore.Reader
	rev datastore.Revision
	p   *nsCachingProxy
}

func (r *nsCachingReader) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	// Check the nsCache.
	nsRevisionKey := nsName + "@" + r.rev.String()

	loadedRaw, found := r.p.c.Get(nsRevisionKey)
	if !found {
		// We couldn't use the cached entry, load one
		var err error
		loadedRaw, err, _ = r.p.readNsGroup.Do(nsRevisionKey, func() (any, error) {
			// sever the context so that another branch doesn't cancel the
			// single-flighted namespace read
			loaded, updatedRev, err := r.Reader.ReadNamespace(SeparateContextWithTracing(ctx), nsName)
			if err != nil && !errors.Is(err, &datastore.ErrNamespaceNotFound{}) {
				// Propagate this error to the caller
				return nil, err
			}

			entry := &cacheEntry{loaded, updatedRev, err}
			r.p.c.Set(nsRevisionKey, entry, entry.Size())

			// We have to call wait here or else Ristretto may not have the key
			// available to a subsequent caller.
			r.p.c.Wait()

			return entry, nil
		})
		if err != nil {
			return nil, datastore.NoRevision, err
		}
	}

	loaded := loadedRaw.(*cacheEntry)
	return loaded.namespaceDefinition, loaded.updated, loaded.notFound
}

type nsCachingRWT struct {
	datastore.ReadWriteTransaction
	namespaceCache *sync.Map
}

type rwtCacheEntry struct {
	loaded   *core.NamespaceDefinition
	updated  datastore.Revision
	notFound error
}

func (rwt *nsCachingRWT) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	untypedEntry, ok := rwt.namespaceCache.Load(nsName)

	var entry rwtCacheEntry
	if ok {
		entry = untypedEntry.(rwtCacheEntry)
	} else {
		loaded, updatedRev, err := rwt.ReadWriteTransaction.ReadNamespace(ctx, nsName)
		if err != nil && !errors.As(err, &datastore.ErrNamespaceNotFound{}) {
			// Propagate this error to the caller
			return nil, datastore.NoRevision, err
		}

		entry = rwtCacheEntry{loaded, updatedRev, err}
		rwt.namespaceCache.Store(nsName, entry)
	}

	return entry.loaded, entry.updated, entry.notFound
}

func (rwt *nsCachingRWT) WriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	if err := rwt.ReadWriteTransaction.WriteNamespaces(ctx, newConfigs...); err != nil {
		return err
	}

	for _, nsDef := range newConfigs {
		rwt.namespaceCache.Delete(nsDef.Name)
	}

	return nil
}

type cacheEntry struct {
	namespaceDefinition *core.NamespaceDefinition
	updated             datastore.Revision
	notFound            error
}

func (c *cacheEntry) Size() int64 {
	return estimatedNamespaceDefinitionSize(c.namespaceDefinition.SizeVT()) + int64(unsafe.Sizeof(c))
}

var (
	_ datastore.Datastore = &nsCachingProxy{}
	_ datastore.Reader    = &nsCachingReader{}
)

func estimatedNamespaceDefinitionSize(sizevt int) int64 {
	size := int64(sizevt * namespaceDefinitionSizeVTMultiplier)
	if size < namespaceDefinitionMinimumSize {
		return namespaceDefinitionMinimumSize
	}
	return size
}
