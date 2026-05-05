package datalayer

import (
	"context"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

// WriteStoredSchemaForTest takes a datastore handle and a schema string
// and writes a stored schema with hash to the datastore. Intended as
// a convenient handle for test logic.
func WriteStoredSchemaForTest(ctx context.Context, ds datastore.Datastore, schemaText string) (datastore.Revision, error) {
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("schema"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	if err != nil {
		return nil, err
	}

	// TODO: there may be a helper somewhere that makes this easier.
	schemaDefinitions := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions)+len(compiled.CaveatDefinitions))
	for _, caveatDef := range compiled.CaveatDefinitions {
		schemaDefinitions = append(schemaDefinitions, caveatDef)
	}
	for _, objDef := range compiled.ObjectDefinitions {
		schemaDefinitions = append(schemaDefinitions, objDef)
	}

	// Use WriteBoth mode so the schema is written to both legacy namespace/caveat
	// storage and the new unified StoredSchema storage. This ensures compatibility
	// regardless of which schema mode the server/DataLayer is running in.
	dl := NewDataLayer(ds, WithSchemaMode(SchemaModeReadLegacyWriteBoth))
	rev, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt ReadWriteTransaction) error {
		return rwt.WriteSchema(ctx, schemaDefinitions, schemaText, nil)
	})

	return rev, err
}
