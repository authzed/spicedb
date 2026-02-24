package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ReadStoredSchema reads the unified stored schema from the Spanner schema table.
func (sr spannerReader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	executor := &spannerSchemaReadExecutor{
		txSource: sr.txSource,
	}

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC(chunker)
	return rw.ReadStoredSchema(ctx)
}

// WriteStoredSchema writes the unified stored schema to the Spanner schema table.
func (rwt spannerReadWriteTXN) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	executor := newSpannerChunkedBytesExecutor(rwt.spannerRWT)

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC(chunker)
	if err := rw.WriteStoredSchema(ctx, schema); err != nil {
		return err
	}

	// Write the schema hash to the schema_revision table if present.
	v1 := schema.GetV1()
	if v1 != nil && v1.SchemaHash != "" {
		mutation := spanner.InsertOrUpdate(
			"schema_revision",
			[]string{"name", "schema_hash", "timestamp"},
			[]any{"current", []byte(v1.SchemaHash), spanner.CommitTimestamp},
		)
		if err := rwt.spannerRWT.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
			return fmt.Errorf("failed to write schema hash: %w", err)
		}
	}

	return nil
}
