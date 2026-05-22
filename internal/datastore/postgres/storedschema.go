package postgres

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"

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
	if err := rw.WriteStoredSchema(ctx, schema); err != nil {
		return err
	}

	// Write the schema hash to the schema_revision table.
	v1 := schema.GetV1()
	if v1 == nil {
		return errors.New("stored schema missing v1 data")
	}

	// Mark existing hash rows as deleted.
	markDeleted := psql.Update("schema_revision").
		Set("deleted_xid", rwt.newXID.Uint64).
		Where(sq.Eq{"name": "current"}).
		Where(sq.Eq{"deleted_xid": liveDeletedTxnID})

	sql, args, err := markDeleted.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build schema revision update query: %w", err)
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to mark existing schema revision as deleted: %w", err)
	}

	// Insert the new hash row.
	insertHash := psql.Insert("schema_revision").
		Columns("name", "hash", "created_xid", "deleted_xid").
		Values("current", []byte(v1.SchemaHash), rwt.newXID.Uint64, liveDeletedTxnID).
		Suffix("ON CONFLICT (name, created_xid) DO NOTHING")

	sql, args, err = insertHash.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build schema revision insert query: %w", err)
	}

	if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to insert schema revision hash: %w", err)
	}

	return nil
}
