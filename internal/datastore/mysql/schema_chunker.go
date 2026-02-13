package mysql

import (
	"context"
	"database/sql"
	"errors"

	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	// MySQL LONGBLOB can store up to 4GB, but we use 64KB chunks for safety
	// and to avoid issues with max_allowed_packet settings.
	mysqlMaxChunkSize = 64 * 1024 // 64KB
)

// BaseSchemaChunkerConfig provides the base configuration for MySQL schema chunking.
// MySQL uses smaller chunks (64KB), question mark placeholders, and tombstone-based write mode.
var BaseSchemaChunkerConfig = common.SQLByteChunkerConfig[uint64]{
	TableName:         "schema",
	NameColumn:        "name",
	ChunkIndexColumn:  "chunk_index",
	ChunkDataColumn:   "chunk_data",
	MaxChunkSize:      mysqlMaxChunkSize,
	PlaceholderFormat: sq.Question,
	WriteMode:         common.WriteModeInsertWithTombstones,
	CreatedAtColumn:   "created_transaction",
	DeletedAtColumn:   "deleted_transaction",
	AliveValue:        liveDeletedTxnID,
}

// mysqlChunkedBytesExecutor implements common.ChunkedBytesExecutor for MySQL.
type mysqlChunkedBytesExecutor struct {
	db *sql.DB
}

func newMySQLChunkedBytesExecutor(db *sql.DB) *mysqlChunkedBytesExecutor {
	return &mysqlChunkedBytesExecutor{db: db}
}

func (e *mysqlChunkedBytesExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &mysqlChunkedBytesTransaction{tx: tx}, nil
}

func (e *mysqlChunkedBytesExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	rows, err := e.db.QueryContext(ctx, sql, args...)
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

// mysqlChunkedBytesTransaction implements common.ChunkedBytesTransaction for MySQL.
type mysqlChunkedBytesTransaction struct {
	tx *sql.Tx
}

func (t *mysqlChunkedBytesTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	}

	return t.tx.Commit()
}

func (t *mysqlChunkedBytesTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	}

	return t.tx.Commit()
}

func (t *mysqlChunkedBytesTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.ExecContext(ctx, sql, args...)
	if err != nil {
		return err
	}

	return t.tx.Commit()
}

// GetSchemaChunker returns a SQLByteChunker for the schema table.
// This is exported for testing purposes.
func (mds *mysqlDatastore) GetSchemaChunker() *common.SQLByteChunker[uint64] {
	executor := newMySQLChunkedBytesExecutor(mds.db)
	return common.MustNewSQLByteChunker(
		BaseSchemaChunkerConfig.
			WithTableName(mds.schemaTableName).
			WithExecutor(executor),
	)
}

// mysqlRevisionAwareExecutor wraps the reader's query infrastructure to provide revision-aware chunk reading
type mysqlRevisionAwareExecutor struct {
	txSource    txFactory
	aliveFilter func(sq.SelectBuilder) sq.SelectBuilder
}

func (e *mysqlRevisionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// We don't support transactions for reading
	return nil, errors.New("transactions not supported for revision-aware reads")
}

func (e *mysqlRevisionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	// Apply the alive filter to get chunks that were alive at this revision
	builder = e.aliveFilter(builder)

	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, err
	}

	// Get a transaction to execute the query
	tx, txCleanup, err := e.txSource(ctx)
	if err != nil {
		return nil, err
	}
	defer common.LogOnError(ctx, txCleanup)

	// Execute the query
	rows, err := tx.QueryContext(ctx, sql, args...)
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

	return result, rows.Err()
}

// mysqlTransactionAwareExecutor wraps an existing sql.Tx to provide transaction-aware chunk writing
type mysqlTransactionAwareExecutor struct {
	tx *sql.Tx
}

func newMySQLTransactionAwareExecutor(tx *sql.Tx) *mysqlTransactionAwareExecutor {
	return &mysqlTransactionAwareExecutor{tx: tx}
}

func (e *mysqlTransactionAwareExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	// Return a transaction wrapper that uses the existing transaction
	return &mysqlTransactionAwareTransaction{tx: e.tx}, nil
}

func (e *mysqlTransactionAwareExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	return nil, errors.New("read operations not supported on transaction-aware executor")
}

// mysqlTransactionAwareTransaction implements common.ChunkedBytesTransaction using an existing sql.Tx
// without committing after each operation (unlike mysqlChunkedBytesTransaction)
type mysqlTransactionAwareTransaction struct {
	tx *sql.Tx
}

func (t *mysqlTransactionAwareTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.ExecContext(ctx, sql, args...)
	return err
}

func (t *mysqlTransactionAwareTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.ExecContext(ctx, sql, args...)
	return err
}

func (t *mysqlTransactionAwareTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}

	_, err = t.tx.ExecContext(ctx, sql, args...)
	return err
}
