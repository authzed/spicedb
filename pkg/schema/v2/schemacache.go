package schema

import (
	"sync"

	"github.com/authzed/spicedb/pkg/datastore"
)

var convertedSchemaCacheKey = datastore.NewDerivedCacheKey[*convertedSchemaCache]()

// convertedSchemaCache belongs to one immutable stored-schema version. Publish a
// small holder first so concurrent requests convert that version only once.
type convertedSchemaCache struct {
	once   sync.Once
	schema *Schema
	err    error
}

// CachedSchemaFor returns the shared, lazily converted view of a stored schema.
// Callers must treat the view and its elements as read-only. The view is discarded
// with its owning stored schema, so schema changes cannot reuse an obsolete view.
func CachedSchemaFor(stored *datastore.ReadOnlyStoredSchema) (*Schema, error) {
	cached, err := datastore.LoadOrStoreDerived(stored, convertedSchemaCacheKey, func() *convertedSchemaCache {
		return &convertedSchemaCache{}
	})
	if err != nil {
		return nil, err
	}

	cached.once.Do(func() {
		cached.schema, cached.err = BuildSchemaFromStoredSchema(stored.Get())
	})

	return cached.schema, cached.err
}
