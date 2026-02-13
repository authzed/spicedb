package schemacaching

import (
	"context"
	"errors"
	"sync"
	"unsafe"

	"golang.org/x/sync/singleflight"

	schemautil "github.com/authzed/spicedb/internal/datastore/schema"
	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// definitionCachingProxy is a datastore proxy that caches schema (namespaces and caveat definitions)
// via the supplied cache.
type definitionCachingProxy struct {
	datastore.Datastore
	c         cache.Cache[cache.StringKey, *cacheEntry]
	readGroup singleflight.Group
}

func (p *definitionCachingProxy) Close() error {
	p.c.Close()
	return p.Datastore.Close()
}

func (p *definitionCachingProxy) SnapshotReader(rev datastore.Revision, hash datastore.SchemaHash) datastore.Reader {
	delegateReader := p.Datastore.SnapshotReader(rev, hash)
	return &definitionCachingReader{delegateReader, rev, p}
}

func (p *definitionCachingProxy) ReadWriteTx(
	ctx context.Context,
	f datastore.TxUserFunc,
	opts ...options.RWTOptionsOption,
) (datastore.Revision, error) {
	return p.Datastore.ReadWriteTx(ctx, func(ctx context.Context, delegateRWT datastore.ReadWriteTransaction) error {
		rwt := &definitionCachingRWT{delegateRWT, &sync.Map{}}
		return f(ctx, rwt)
	}, opts...)
}

const (
	namespaceCacheKeyPrefix = "n"
	caveatCacheKeyPrefix    = "c"
)

type definitionCachingReader struct {
	datastore.Reader
	rev datastore.Revision
	p   *definitionCachingProxy
}

func (r *definitionCachingReader) LegacyReadNamespaceByName(
	ctx context.Context,
	name string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	return readAndCache(ctx, r, namespaceCacheKeyPrefix, name,
		func(ctx context.Context, name string) (*core.NamespaceDefinition, datastore.Revision, error) {
			return r.Reader.LegacyReadNamespaceByName(ctx, name)
		},
		estimatedNamespaceDefinitionSize)
}

func (r *definitionCachingReader) LegacyLookupNamespacesWithNames(
	ctx context.Context,
	nsNames []string,
) ([]datastore.RevisionedNamespace, error) {
	return listAndCache(ctx, r, namespaceCacheKeyPrefix, nsNames,
		func(ctx context.Context, names []string) ([]datastore.RevisionedNamespace, error) {
			return r.Reader.LegacyLookupNamespacesWithNames(ctx, names)
		},
		estimatedNamespaceDefinitionSize)
}

func (r *definitionCachingReader) LegacyReadCaveatByName(
	ctx context.Context,
	name string,
) (*core.CaveatDefinition, datastore.Revision, error) {
	return readAndCache(ctx, r, caveatCacheKeyPrefix, name,
		func(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
			return r.Reader.LegacyReadCaveatByName(ctx, name)
		},
		estimatedCaveatDefinitionSize)
}

func (r *definitionCachingReader) LegacyLookupCaveatsWithNames(
	ctx context.Context,
	caveatNames []string,
) ([]datastore.RevisionedCaveat, error) {
	return listAndCache(ctx, r, caveatCacheKeyPrefix, caveatNames,
		func(ctx context.Context, names []string) ([]datastore.RevisionedCaveat, error) {
			return r.Reader.LegacyLookupCaveatsWithNames(ctx, names)
		},
		estimatedCaveatDefinitionSize)
}

// SchemaReader returns a schema reader that respects the underlying datastore's
// schema mode configuration. For new unified schema mode, it passes through directly
// to leverage the hash-based cache. For legacy mode, it wraps the proxy to use
// the per-definition caching methods.
func (r *definitionCachingReader) SchemaReader() (datastore.SchemaReader, error) {
	// Get the underlying reader's schema reader to determine its configured mode
	underlyingSchemaReader, err := r.Reader.SchemaReader()
	if err != nil {
		return nil, err
	}

	// If using new unified schema mode, pass through directly
	// The hash-based schema cache handles caching efficiently for unified schemas
	if _, isLegacy := underlyingSchemaReader.(*schemautil.LegacySchemaReaderAdapter); !isLegacy {
		return underlyingSchemaReader, nil
	}

	// For legacy mode, wrap the proxy to ensure per-definition caching is used
	return schemautil.NewLegacySchemaReaderAdapter(r), nil
}

func (r *definitionCachingReader) ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error) {
	singleStoreReader, ok := r.Reader.(datastore.SingleStoreSchemaReader)
	if !ok {
		return nil, errors.New("delegate reader does not implement SingleStoreSchemaReader")
	}
	return singleStoreReader.ReadStoredSchema(ctx)
}

func listAndCache[T schemaDefinition](
	ctx context.Context,
	r *definitionCachingReader,
	prefix string,
	names []string,
	reader func(ctx context.Context, names []string) ([]datastore.RevisionedDefinition[T], error),
	estimator func(sizeVT int) int64,
) ([]datastore.RevisionedDefinition[T], error) {
	if len(names) == 0 {
		return nil, nil
	}

	// Check the cache for each entry.
	remainingToLoad := mapz.NewSet[string]()
	remainingToLoad.Extend(names)

	foundDefs := make([]datastore.RevisionedDefinition[T], 0, len(names))
	for _, name := range names {
		cacheRevisionKey := prefix + ":" + name + "@" + r.rev.String()
		loaded, found := r.p.c.Get(cache.StringKey(cacheRevisionKey))
		if !found {
			continue
		}

		remainingToLoad.Delete(name)

		if loaded.notFound != nil {
			// If the value was in the cache, but its notFound is defined (implying that
			// it was not found on a previous lookup), we pass over it.
			// We still remove it from the `remainingToLoad` set because
			// we don't want to go to the datastore for it.
			continue
		}

		foundDefs = append(foundDefs, datastore.RevisionedDefinition[T]{
			Definition:          loaded.definition.(T),
			LastWrittenRevision: loaded.updated,
		})
	}

	if !remainingToLoad.IsEmpty() {
		// Load and cache the remaining names.
		loadedDefs, err := reader(ctx, remainingToLoad.AsSlice())
		if err != nil {
			return nil, err
		}

		for _, def := range loadedDefs {
			foundDefs = append(foundDefs, def)

			cacheRevisionKey := prefix + ":" + def.Definition.GetName() + "@" + r.rev.String()
			estimatedDefinitionSize := estimator(def.Definition.SizeVT())
			entry := &cacheEntry{def.Definition, def.LastWrittenRevision, estimatedDefinitionSize, err}
			r.p.c.Set(cache.StringKey(cacheRevisionKey), entry, entry.Size())
		}

		// We have to call wait here or else Ristretto may not have the key(s)
		// available to a subsequent caller.
		r.p.c.Wait()
	}

	return foundDefs, nil
}

func readAndCache[T schemaDefinition](
	ctx context.Context,
	r *definitionCachingReader,
	prefix string,
	name string,
	reader func(ctx context.Context, name string) (T, datastore.Revision, error),
	estimator func(sizeVT int) int64,
) (T, datastore.Revision, error) {
	// Check the cache.
	cacheRevisionKey := prefix + ":" + name + "@" + r.rev.String()
	loaded, found := r.p.c.Get(cache.StringKey(cacheRevisionKey))
	if !found {
		// We couldn't use the cached entry, load one
		var err error
		loadedRaw, err, _ := r.p.readGroup.Do(cacheRevisionKey, func() (any, error) {
			// sever the context so that another branch doesn't cancel the
			// single-flighted read
			loaded, updatedRev, err := reader(context.WithoutCancel(ctx), name)
			if err != nil && !errors.As(err, &datastore.NamespaceNotFoundError{}) && !errors.As(err, &datastore.CaveatNameNotFoundError{}) {
				// Propagate this error to the caller
				return nil, err
			}

			estimatedDefinitionSize := estimator(loaded.SizeVT())
			entry := &cacheEntry{loaded, updatedRev, estimatedDefinitionSize, err}
			r.p.c.Set(cache.StringKey(cacheRevisionKey), entry, entry.Size())

			// We have to call wait here or else Ristretto may not have the key
			// available to a subsequent caller.
			r.p.c.Wait()
			return entry, nil
		})
		if err != nil {
			return *new(T), datastore.NoRevision, err
		}

		loaded = loadedRaw.(*cacheEntry)
	}

	return loaded.definition.(T), loaded.updated, loaded.notFound
}

type definitionCachingRWT struct {
	datastore.ReadWriteTransaction
	definitionCache *sync.Map
}

type definitionEntry struct {
	loaded   schemaDefinition
	updated  datastore.Revision
	notFound error
}

func (rwt *definitionCachingRWT) LegacyReadNamespaceByName(
	ctx context.Context,
	nsName string,
) (*core.NamespaceDefinition, datastore.Revision, error) {
	return readAndCacheInTransaction(
		ctx, rwt, "namespace", nsName, func(ctx context.Context, name string) (*core.NamespaceDefinition, datastore.Revision, error) {
			return rwt.ReadWriteTransaction.LegacyReadNamespaceByName(ctx, name)
		})
}

func (rwt *definitionCachingRWT) LegacyReadCaveatByName(
	ctx context.Context,
	nsName string,
) (*core.CaveatDefinition, datastore.Revision, error) {
	return readAndCacheInTransaction(
		ctx, rwt, "caveat", nsName, func(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
			return rwt.ReadWriteTransaction.LegacyReadCaveatByName(ctx, name)
		})
}

func readAndCacheInTransaction[T schemaDefinition](
	ctx context.Context,
	rwt *definitionCachingRWT,
	prefix string,
	name string,
	reader func(ctx context.Context, name string) (T, datastore.Revision, error),
) (T, datastore.Revision, error) {
	key := prefix + ":" + name
	untypedEntry, ok := rwt.definitionCache.Load(key)

	var entry definitionEntry
	if ok {
		entry = untypedEntry.(definitionEntry)
	} else {
		loaded, updatedRev, err := reader(ctx, name)
		if err != nil && !errors.As(err, &datastore.NamespaceNotFoundError{}) && !errors.As(err, &datastore.CaveatNameNotFoundError{}) {
			// Propagate this error to the caller
			return *new(T), datastore.NoRevision, err
		}

		entry = definitionEntry{loaded, updatedRev, err}
		rwt.definitionCache.Store(key, entry)
	}

	return entry.loaded.(T), entry.updated, entry.notFound
}

func (rwt *definitionCachingRWT) LegacyWriteNamespaces(ctx context.Context, newConfigs ...*core.NamespaceDefinition) error {
	if err := rwt.ReadWriteTransaction.LegacyWriteNamespaces(ctx, newConfigs...); err != nil {
		return err
	}

	for _, nsDef := range newConfigs {
		rwt.definitionCache.Delete("namespace:" + nsDef.Name)
	}

	return nil
}

func (rwt *definitionCachingRWT) LegacyWriteCaveats(ctx context.Context, newConfigs []*core.CaveatDefinition) error {
	if err := rwt.ReadWriteTransaction.LegacyWriteCaveats(ctx, newConfigs); err != nil {
		return err
	}

	for _, caveatDef := range newConfigs {
		rwt.definitionCache.Delete("caveat:" + caveatDef.Name)
	}

	return nil
}

// SchemaWriter returns a wrapper around the definitionCachingRWT that ensures
// that the caching logic in this proxy is exercised when a handle on the
// SchemaWriter is requested.
func (rwt *definitionCachingRWT) SchemaWriter() (datastore.SchemaWriter, error) {
	return schemautil.NewLegacySchemaWriterAdapter(rwt, rwt.ReadWriteTransaction), nil
}

// SchemaReader returns a schema reader for the transaction. For new unified schema mode,
// it passes through directly. For legacy mode, it wraps the transaction to use caching.
func (rwt *definitionCachingRWT) SchemaReader() (datastore.SchemaReader, error) {
	underlyingSchemaReader, err := rwt.ReadWriteTransaction.SchemaReader()
	if err != nil {
		return nil, err
	}

	// If using new unified schema mode, pass through directly
	if _, isLegacy := underlyingSchemaReader.(*schemautil.LegacySchemaReaderAdapter); !isLegacy {
		return underlyingSchemaReader, nil
	}

	// For legacy mode, wrap to use transaction-local caching
	return schemautil.NewLegacySchemaReaderAdapter(rwt), nil
}

type cacheEntry struct {
	definition              schemaDefinition
	updated                 datastore.Revision
	estimatedDefinitionSize int64
	notFound                error
}

func (c *cacheEntry) Size() int64 {
	return c.estimatedDefinitionSize + int64(unsafe.Sizeof(c))
}

var (
	_ datastore.Datastore               = &definitionCachingProxy{}
	_ datastore.Reader                  = &definitionCachingReader{}
	_ datastore.LegacySchemaReader      = &definitionCachingReader{}
	_ datastore.SingleStoreSchemaReader = &definitionCachingReader{}
	_ datastore.DualSchemaReader        = &definitionCachingReader{}
	_ datastore.ReadWriteTransaction    = &definitionCachingRWT{}
)

func estimatedNamespaceDefinitionSize(sizevt int) int64 {
	size := int64(sizevt * namespaceDefinitionSizeVTMultiplier)
	if size < namespaceDefinitionMinimumSize {
		return namespaceDefinitionMinimumSize
	}
	return size
}

func estimatedCaveatDefinitionSize(sizevt int) int64 {
	size := int64(sizevt * caveatDefinitionSizeVTMultiplier)
	if size < caveatDefinitionMinimumSize {
		return caveatDefinitionMinimumSize
	}
	return size
}
