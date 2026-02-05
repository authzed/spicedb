package postgres

import (
	"context"
	"errors"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/authzed/spicedb/internal/datastore/common"
	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
)

const (
	// PostgreSQL has no practical limit on BYTEA column size (up to 1GB per cell),
	// but we use 1MB chunks for reasonable memory usage and query performance.
	postgresMaxChunkSize = 1024 * 1024 // 1MB
)

// BaseSchemaChunkerConfig provides the base configuration for Postgres schema chunking.
// Postgres uses tombstone-based write mode with XID8 columns (matching relationship tables).
var BaseSchemaChunkerConfig = common.SQLByteChunkerConfig[uint64]{
	TableName:         "schema",
	NameColumn:        "name",
	ChunkIndexColumn:  "chunk_index",
	ChunkDataColumn:   "chunk_data",
	MaxChunkSize:      postgresMaxChunkSize,
	PlaceholderFormat: sq.Dollar,
	WriteMode:         common.WriteModeInsertWithTombstones,
	CreatedAtColumn:   "created_xid",
	DeletedAtColumn:   "deleted_xid",
	AliveValue:        liveDeletedTxnID,
}

// postgresChunkedBytesExecutor implements common.ChunkedBytesExecutor for PostgreSQL.
type postgresChunkedBytesExecutor struct {
	pool *pgxpool.Pool
}

func newPostgresChunkedBytesExecutor(pool *pgxpool.Pool) *postgresChunkedBytesExecutor {
	return &postgresChunkedBytesExecutor{pool: pool}
}

func (e *postgresChunkedBytesExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &postgresChunkedBytesTransaction{tx: tx}, nil
}

func (e *postgresChunkedBytesExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := e.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int][]byte)
	for rows.Next() {
		var chunkIndex int
		var chunkData []byte
		if err := rows.Scan(&chunkIndex, &chunkData); err != nil {
			return nil, err
		}
		result[chunkIndex] = chunkData
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

// postgresChunkedBytesTransaction implements common.ChunkedBytesTransaction for PostgreSQL.
type postgresChunkedBytesTransaction struct {
	tx pgx.Tx
}

func (t *postgresChunkedBytesTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}

func (t *postgresChunkedBytesTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}

func (t *postgresChunkedBytesTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.Exec(ctx, sql, args...)
	return err
}

// GetSchemaChunker returns a SQLByteChunker for the schema table.
// This is exported for testing purposes.
func (pgd *pgDatastore) GetSchemaChunker() *common.SQLByteChunker[uint64] {
	executor := newPostgresChunkedBytesExecutor(pgd.readPool.(*pgxpool.Pool))
	return common.MustNewSQLByteChunker(BaseSchemaChunkerConfig.WithExecutor(executor))
}

// pgRevisionAwareExecutor wraps the reader's query infrastructure to provide revision-aware chunk reading
type pgRevisionAwareExecutor struct {
	query       pgxcommon.DBFuncQuerier
	aliveFilter func(sq.SelectBuilder) sq.SelectBuilder
}

func (e *pgRevisionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// We don't support transactions for reading
	return nil, errors.New("transactions not supported for revision-aware reads")
}

func (e *pgRevisionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	// Apply the alive filter to get chunks that were alive at this revision
	builder = e.aliveFilter(builder)

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

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

// pgTransactionAwareExecutor wraps an existing pgx.Tx to provide transaction-aware chunk writing
type pgTransactionAwareExecutor struct {
	tx pgx.Tx
}

func newPGTransactionAwareExecutor(tx pgx.Tx) *pgTransactionAwareExecutor {
	return &pgTransactionAwareExecutor{tx: tx}
}

func (e *pgTransactionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// Return a transaction wrapper that uses the existing transaction
	return &postgresChunkedBytesTransaction{tx: e.tx}, nil
}

func (e *pgTransactionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	return nil, errors.New("read operations not supported on transaction-aware executor")
}
