package query

import (
	"context"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
)

// LoadSchema returns the read-only schema view used for query planning and
// deserialization. Single-store readers reuse a view owned by the stored schema;
// legacy readers convert their revision's definitions without retaining a view.
func LoadSchema(ctx context.Context, reader datalayer.SchemaReader) (*schema.Schema, error) {
	if stored, ok := reader.(interface {
		StoredSchema() *datastore.ReadOnlyStoredSchema
	}); ok {
		return schema.CachedSchemaFor(stored.StoredSchema())
	}

	namespaces, err := reader.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	caveats, err := reader.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	return schema.BuildSchemaFromDefinitions(datastore.DefinitionsOf(namespaces), datastore.DefinitionsOf(caveats))
}
