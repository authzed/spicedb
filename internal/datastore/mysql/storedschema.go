package mysql

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const schemaRevisionName = "current"

// ReadStoredSchema reads the unified stored schema from the MySQL schema table.
func (mr *mysqlReader) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	executor := &mysqlRevisionAwareExecutor{
		txSource:    mr.txSource,
		aliveFilter: mr.aliveFilter,
	}

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return nil, fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterForTransactionIDs(chunker, common.NoTransactionID[uint64])
	return rw.ReadStoredSchema(ctx)
}

// WriteStoredSchema writes the unified stored schema to the MySQL schema table.
func (rwt *mysqlReadWriteTXN) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	executor := newMySQLTransactionAwareExecutor(rwt.tx)

	chunker, err := common.NewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
	if err != nil {
		return fmt.Errorf("failed to create schema chunker: %w", err)
	}

	rw := common.NewSQLSingleStoreSchemaReaderWriterForTransactionIDs(chunker, func(_ context.Context) uint64 {
		return rwt.newTxnID
	})
	if err := rw.WriteStoredSchema(ctx, schema); err != nil {
		return err
	}

	// Write the schema hash to the schema_revision table if available.
	v1 := schema.GetV1()
	if v1 != nil && v1.SchemaHash != "" {
		// Mark existing hash rows as deleted (tombstone pattern).
		delQuery, delArgs, err := sb.Update(rwt.schemaRevisionTableName).
			Set(colDeletedTxn, rwt.newTxnID).
			Where(sq.Eq{colName: schemaRevisionName}).
			Where(sq.Eq{colDeletedTxn: liveDeletedTxnID}).
			ToSql()
		if err != nil {
			return fmt.Errorf("failed to build schema revision delete query: %w", err)
		}

		if _, err := rwt.tx.ExecContext(ctx, delQuery, delArgs...); err != nil {
			return fmt.Errorf("failed to tombstone existing schema revision: %w", err)
		}

		// Insert the new hash row. Use INSERT IGNORE for idempotency.
		insQuery, insArgs, err := sb.Insert(rwt.schemaRevisionTableName).
			Options("IGNORE").
			Columns(colName, "hash", colCreatedTxn, colDeletedTxn).
			Values(schemaRevisionName, []byte(v1.SchemaHash), rwt.newTxnID, liveDeletedTxnID).
			ToSql()
		if err != nil {
			return fmt.Errorf("failed to build schema revision insert query: %w", err)
		}

		if _, err := rwt.tx.ExecContext(ctx, insQuery, insArgs...); err != nil {
			return fmt.Errorf("failed to insert schema revision: %w", err)
		}
	}

	return nil
}
