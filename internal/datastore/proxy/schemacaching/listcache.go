package schemacaching

import (
	"context"
	"slices"
	"unsafe"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datastore"
)

// LegacyListAllNamespaces caches the complete namespace listing at this exact revision.
func (r *definitionCachingReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	return listAllAndCache(ctx, r, namespaceCacheKeyPrefix, r.Reader.LegacyListAllNamespaces, estimatedNamespaceDefinitionSize)
}

// LegacyListAllCaveats caches the complete caveat listing at this exact revision.
func (r *definitionCachingReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	return listAllAndCache(ctx, r, caveatCacheKeyPrefix, r.Reader.LegacyListAllCaveats, estimatedCaveatDefinitionSize)
}

func listAllAndCache[T schemaDefinition](
	ctx context.Context,
	r *definitionCachingReader,
	prefix string,
	reader func(context.Context) ([]datastore.RevisionedDefinition[T], error),
	estimator func(int) int64,
) ([]datastore.RevisionedDefinition[T], error) {
	// Full listings need their own key: a collection of individually cached names
	// cannot prove that no additional definition exists at the requested revision.
	// Exact revision identity also preserves deletions and historical schema reads.
	key := cache.StringKey("all:" + prefix + "@" + r.rev.String())
	loaded, found := r.p.c.Get(key)
	if !found {
		entry, _, err := r.p.readGroup.Do(ctx, string(key), func(ctx context.Context) (*cacheEntry, error) {
			// Another caller may have filled the cache between our miss and flight admission.
			// Rechecking avoids an unnecessary load after an earlier flight has completed.
			if existing, ok := r.p.c.Get(key); ok {
				return existing, nil
			}

			definitions, err := reader(ctx)
			if err != nil {
				return nil, err
			}

			// Account for the retained slice and every definition in the same byte budget
			// as named reads. Empty successful lists are useful cache entries too; errors
			// must retry the backend rather than becoming permanent empty-schema results.
			cost := int64(unsafe.Sizeof(cacheEntry{})) + int64(unsafe.Sizeof(definitions))

			for _, definition := range definitions {
				cost += estimator(definition.Definition.SizeVT()) + safecast.MustConvert[int64](unsafe.Sizeof(definition))
			}

			entry := &cacheEntry{
				definitions:             slices.Clone(definitions),
				estimatedDefinitionSize: cost,
			}
			r.p.c.Set(key, entry, entry.Size())
			return entry, nil
		})
		if err != nil {
			return nil, err
		}

		loaded = entry
	}

	// Definitions follow the existing cache's read-only proto contract. A fresh
	// slice lets callers reorder or replace elements without corrupting later reads.
	return slices.Clone(loaded.definitions.([]datastore.RevisionedDefinition[T])), nil
}

// LegacyListAllNamespaces uses the revision cache because the watch index tracks
// individual names and cannot establish completeness of a historical listing.
func (w *watchingCachingReader) LegacyListAllNamespaces(ctx context.Context) ([]datastore.RevisionedNamespace, error) {
	reader := &definitionCachingReader{Reader: w.Reader, rev: w.rev, p: w.p.fallbackCache}
	return reader.LegacyListAllNamespaces(ctx)
}

// LegacyListAllCaveats shares the fallback cache's complete revisioned listings.
func (w *watchingCachingReader) LegacyListAllCaveats(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	reader := &definitionCachingReader{Reader: w.Reader, rev: w.rev, p: w.p.fallbackCache}
	return reader.LegacyListAllCaveats(ctx)
}
