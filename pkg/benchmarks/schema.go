package benchmarks

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
)

// ReadSchema reads all namespace and caveat definitions from the datastore at
// the given revision and returns the compiled schema.
func ReadSchema(ctx context.Context, ds datastore.Datastore, rev datastore.Revision) (*schema.Schema, error) {
	reader := ds.SnapshotReader(rev)

	storedSchema, err := reader.ReadStoredSchema(ctx)
	if err != nil {
		return nil, err
	}

	return schema.BuildSchemaFromStoredSchema(storedSchema.Get())
}
