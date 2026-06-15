package crdb

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

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

// assertSchemaHash verifies the schema_revision row matches expectedHash.
// Under CRDB's default SERIALIZABLE isolation, both FOR UPDATE and FOR SHARE are unreplicated
// best-effort locks that cannot be relied upon for correctness. Correctness for concurrent
// schema and relationship writes is instead guaranteed by CRDB's serializable conflict
// detection at commit time. The exclusive flag is ignored.
func assertSchemaHash(ctx context.Context, tx pgx.Tx, expectedHash string) error {
	q := psql.Select("hash").
		From("schema_revision").
		Where(sq.Eq{"name": "current"})
	sqlStr, args, err := q.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build schema hash precondition query: %w", err)
	}

	var storedHash []byte
	if err := tx.QueryRow(ctx, sqlStr, args...).Scan(&storedHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return datastore.ErrSchemaNotFound
		}
		return fmt.Errorf("failed to check schema hash precondition: %w", err)
	}
	if string(storedHash) != expectedHash {
		return datastore.ErrSchemaHashPreconditionFailed
	}
	return nil
}

// WriteStoredSchema writes the unified stored schema to the CRDB schema table.
func (rwt *crdbReadWriteTXN) WriteStoredSchema(ctx context.Context, schema *core.StoredSchema) error {
	executor := newTransactionAwareExecutor(rwt.tx)

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
		sql, args, err := psql.Insert("schema_revision").
			Columns("name", "hash", "timestamp").
			Values("current", []byte(v1.SchemaHash), sq.Expr("now()")).
			Suffix("ON CONFLICT (name) DO UPDATE SET hash = EXCLUDED.hash, timestamp = EXCLUDED.timestamp").
			ToSql()
		if err != nil {
			return fmt.Errorf("error building schema revision upsert: %w", err)
		}

		if _, err := rwt.tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("error writing schema revision: %w", err)
		}
	}

	return nil
}
