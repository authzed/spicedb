package postgres

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// ReadStoredSchema reads the unified stored schema from the Postgres schema table.
func (r *pgReader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	executor := &pgRevisionAwareExecutor{
		query:       r.query,
		aliveFilter: r.aliveFilter,
	}

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterForTransactionIDs(chunker, common.NoTransactionID[uint64])
	return rw.ReadStoredSchema(ctx)
}

// WriteStoredSchema writes the unified stored schema to the Postgres schema table.
func (rwt *pgReadWriteTXN) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	executor := newPGTransactionAwareExecutor(rwt.tx)

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterForTransactionIDs(chunker, func(_ context.Context) uint64 {
		return rwt.newXID.Uint64
	})
	return rw.WriteStoredSchema(ctx, schema)
}
