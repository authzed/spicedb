package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/singleflight"

	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// NewCachingDatastoreProxy creates a new datastore proxy which caches namespace definitions that
// are loaded at specific datastore revisions.
func NewCachingDatastoreProxy(
	delegate datastore.Datastore,
	cacheConfig *cache.Config,
) (datastore.Datastore, error) {
	if cacheConfig == nil {
		cacheConfig = &cache.Config{
			NumCounters: 1e4,     // number of keys to track frequency of (10k).
			MaxCost:     1 << 24, // maximum cost of cache (16MB).
			BufferItems: 64,      // number of keys per Get buffer.
		}
	} else {
		log.Info().Int64("numCounters", cacheConfig.NumCounters).Str("maxCost", humanize.Bytes(uint64(cacheConfig.MaxCost))).Msg("configured caching namespace manager")
	}

	cache, err := cache.NewCache(cacheConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create cache: %w", err)
	}

	return &nsCachingProxy{
		Datastore: delegate,
		c:         cache,
	}, nil
}

type nsCachingProxy struct {
	datastore.Datastore
	c           cache.Cache
	readNsGroup singleflight.Group
}

func (p *nsCachingProxy) SnapshotReader(rev datastore.Revision) datastore.Reader {
	delegateReader := p.Datastore.SnapshotReader(rev)
	return &nsCachingReader{delegateReader, sync.Mutex{}, rev, p}
}

func (p *nsCachingProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
) (decimal.Decimal, error) {
	return p.Datastore.ReadWriteTx(ctx, func(ctx context.Context, delegateRWT datastore.ReadWriteTransaction) error {
		rwt := &nsCachingRWT{delegateRWT, &sync.Map{}}
		return f(ctx, rwt)
	})
}

type nsCachingReader struct {
	datastore.Reader
	sync.Mutex
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
			loaded, updatedRev, err := r.Reader.ReadNamespace(ctx, nsName)
			if err != nil && !errors.Is(err, &datastore.ErrNamespaceNotFound{}) {
				// Propagate this error to the caller
				return nil, err
			}

			entry := &cacheEntry{loaded, updatedRev, err}
			r.p.c.Set(nsRevisionKey, entry, int64(loaded.SizeVT()))

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
	return loaded.def, loaded.updated, loaded.notFound
}

type nsCachingRWT struct {
	datastore.ReadWriteTransaction
	namespaceCache *sync.Map
}

func (rwt *nsCachingRWT) ReadNamespace(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	untypedEntry, ok := rwt.namespaceCache.Load(nsName)

	var entry cacheEntry
	if ok {
		entry = untypedEntry.(cacheEntry)
	} else {
		loaded, updatedRev, err := rwt.ReadWriteTransaction.ReadNamespace(ctx, nsName)
		if err != nil && !errors.Is(err, &datastore.ErrNamespaceNotFound{}) {
			// Propagate this error to the caller
			return nil, datastore.NoRevision, err
		}

		entry = cacheEntry{loaded, updatedRev, err}
		rwt.namespaceCache.Store(nsName, entry)
	}

	return entry.def, entry.updated, entry.notFound
}

type cacheEntry struct {
	def      *core.NamespaceDefinition
	updated  datastore.Revision
	notFound error
}

var (
	_ datastore.Datastore = &nsCachingProxy{}
	_ datastore.Reader    = &nsCachingReader{}
)
