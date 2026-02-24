package crdb

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ReadStoredSchema reads the unified stored schema from the CRDB schema table.
func (cr *crdbReader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	executor := &revisionAwareExecutor{
		query: cr.query,
		addFromToQuery: func(builder sq.SelectBuilder, tableName string, indexHint string) sq.SelectBuilder {
			return cr.addFromToQuery(builder, tableName, indexHint)
		},
		assertAsOfSysTime: func(_ string) {
			// No-op: the addFromToQuery already adds AS OF SYSTEM TIME
		},
	}

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC(chunker)
	return rw.ReadStoredSchema(ctx)
}

// WriteStoredSchema writes the unified stored schema to the CRDB schema table.
func (rwt *crdbReadWriteTXN) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	executor := newTransactionAwareExecutor(rwt.tx)

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC(chunker)
	return rw.WriteStoredSchema(ctx, schema)
}
