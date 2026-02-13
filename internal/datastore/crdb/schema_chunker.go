package crdb

import (
	"context"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/crdb/pool"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
)

const (
	// CockroachDB has no practical limit on BYTEA column size (similar to Postgres),
	// but we use 1MB chunks for reasonable memory usage and query performance.
	crdbMaxChunkSize = 1024 * 1024 // 1MB
)

// BaseSchemaChunkerConfig provides the base configuration for CRDB schema chunking.
// CRDB uses delete-and-insert write mode since it handles MVCC automatically.
var BaseSchemaChunkerConfig = common.SQLByteChunkerConfig[any]{
	TableName:         "schema",
	NameColumn:        "name",
	ChunkIndexColumn:  "chunk_index",
	ChunkDataColumn:   "chunk_data",
	MaxChunkSize:      crdbMaxChunkSize,
	PlaceholderFormat: sq.Dollar,
	WriteMode:         common.WriteModeDeleteAndInsert,
}

// crdbChunkedBytesExecutor implements common.ChunkedBytesExecutor for CockroachDB.
type crdbChunkedBytesExecutor struct {
	pool *pool.RetryPool
}

func newCRDBChunkedBytesExecutor(pool *pool.RetryPool) *crdbChunkedBytesExecutor {
	return &crdbChunkedBytesExecutor{pool: pool}
}

func (e *crdbChunkedBytesExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// For CRDB, we'll use BeginFunc which provides automatic retry logic
	return &crdbChunkedBytesTransaction{pool: e.pool, ctx: ctx}, nil
}

func (e *crdbChunkedBytesExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	result := make(map[int][]byte)
	err = e.pool.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var chunkIndex int
			var chunkData []byte
			if err := rows.Scan(&chunkIndex, &chunkData); err != nil {
				return err
			}
			result[chunkIndex] = chunkData
		}
		return rows.Err()
	}, sql, args...)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// crdbChunkedBytesTransaction implements common.ChunkedBytesTransaction for CockroachDB.
type crdbChunkedBytesTransaction struct {
	pool *pool.RetryPool
	ctx  context.Context
}

func (t *crdbChunkedBytesTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	return t.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, sql, args...)
		return err
	})
}

func (t *crdbChunkedBytesTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	return t.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, sql, args...)
		return err
	})
}

func (t *crdbChunkedBytesTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	return t.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, sql, args...)
		return err
	})
}

// GetSchemaChunker returns a SQLByteChunker for the schema table.
// This is exported for testing purposes.
func (cds *crdbDatastore) GetSchemaChunker() *common.SQLByteChunker[any] {
	executor := newCRDBChunkedBytesExecutor(cds.readPool)
	return common.MustNewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
}

// revisionAwareExecutor wraps the reader's query infrastructure to provide revision-aware chunk reading
type revisionAwareExecutor struct {
	query             pgxcommon.DBFuncQuerier
	addFromToQuery    func(sq.SelectBuilder, string, string) sq.SelectBuilder
	assertAsOfSysTime func(string)
}

func (e *revisionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// We don't support transactions for reading
	return nil, errors.New("transactions not supported for revision-aware reads")
}

func (e *revisionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	// Modify the builder to add AS OF SYSTEM TIME
	builder = e.addFromToQuery(builder, "schema", "")

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}
	e.assertAsOfSysTime(sql)

	// Execute using the reader's query function
	result := make(map[int][]byte)
	err = e.query.QueryFunc(ctx, func(ctx context.Context, rows pgx.Rows) error {
		for rows.Next() {
			var chunkIndex int
			var chunkData []byte
			if err := rows.Scan(&chunkIndex, &chunkData); err != nil {
				return err
			}
			result[chunkIndex] = chunkData
		}
		return rows.Err()
	}, sql, args...)

	return result, err
}

// transactionAwareExecutor wraps an existing pgx.Tx to provide transaction-aware chunk writing
type transactionAwareExecutor struct {
	tx pgx.Tx
}

func newTransactionAwareExecutor(tx pgx.Tx) *transactionAwareExecutor {
	return &transactionAwareExecutor{tx: tx}
}

func (e *transactionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// Return a transaction wrapper that uses the existing transaction
	return &transactionAwareTransaction{tx: e.tx}, nil
}

func (e *transactionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	return nil, errors.New("read operations not supported on transaction-aware executor")
}

// transactionAwareTransaction implements common.ChunkedBytesTransaction using an existing pgx.Tx
type transactionAwareTransaction struct {
	tx pgx.Tx
}

func (t *transactionAwareTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}

func (t *transactionAwareTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}

func (t *transactionAwareTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}
