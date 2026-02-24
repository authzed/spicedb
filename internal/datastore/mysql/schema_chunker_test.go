package mysql

import (
	"context"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
)

// fakeTransaction captures SQL and args via builder.ToSql() for verification.
type fakeTransaction struct {
	capturedSQL   []string
	capturedArgs  [][]any
	updateQueries []string
}

func (f *fakeTransaction) ExecuteWrite(_ context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
	return nil
}

func (f *fakeTransaction) ExecuteDelete(_ context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
	return nil
}

func (f *fakeTransaction) ExecuteUpdate(_ context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
	f.updateQueries = append(f.updateQueries, sql)
	return nil
}

// fakeExecutor returns the fakeTransaction from BeginTransaction.
type fakeExecutor struct {
	transaction *fakeTransaction
	readResult  map[int][]byte
}

func (e *fakeExecutor) BeginTransaction(_ context.Context) (common.ChunkedBytesTransaction, error) {
	return e.transaction, nil
}

func (e *fakeExecutor) ExecuteRead(_ context.Context, _ sq.SelectBuilder) (map[int][]byte, error) {
	return e.readResult, nil
}

func TestBaseSchemaChunkerConfig(t *testing.T) {
	require.Equal(t, "stored_schema", BaseSchemaChunkerConfig.TableName)
	require.Equal(t, "name", BaseSchemaChunkerConfig.NameColumn)
	require.Equal(t, "chunk_index", BaseSchemaChunkerConfig.ChunkIndexColumn)
	require.Equal(t, "chunk_data", BaseSchemaChunkerConfig.ChunkDataColumn)
	require.Equal(t, 64*1024, BaseSchemaChunkerConfig.MaxChunkSize)
	require.Equal(t, sq.Question, BaseSchemaChunkerConfig.PlaceholderFormat)
	require.Equal(t, common.WriteModeInsertWithTombstones, BaseSchemaChunkerConfig.WriteMode)
}

func TestWrite(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}

	config := BaseSchemaChunkerConfig.WithExecutor(executor)
	chunker := common.MustNewSQLByteChunker(config)

	createdAt := uint64(100)
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", []byte("hello"), createdAt)
	require.NoError(t, err)

	// Should have UPDATE (tombstone) + INSERT
	require.Len(t, txn.capturedSQL, 2)
	require.Len(t, txn.updateQueries, 1)

	// UPDATE uses ? placeholders
	require.Contains(t, txn.capturedSQL[0], "UPDATE stored_schema")
	require.Contains(t, txn.capturedSQL[0], "SET deleted_transaction = ?")

	// INSERT uses ? placeholders
	require.Contains(t, txn.capturedSQL[1], "INSERT INTO stored_schema")
	require.Contains(t, txn.capturedSQL[1], "?")
}

func TestDelete(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}

	config := BaseSchemaChunkerConfig.WithExecutor(executor)
	chunker := common.MustNewSQLByteChunker(config)

	deletedAt := uint64(200)
	err := chunker.DeleteChunkedBytes(context.Background(), "test-key", deletedAt)
	require.NoError(t, err)

	// Should have UPDATE (tombstone)
	require.Len(t, txn.capturedSQL, 1)
	require.Len(t, txn.updateQueries, 1)
	require.Contains(t, txn.capturedSQL[0], "UPDATE stored_schema")
	require.Contains(t, txn.capturedSQL[0], "SET deleted_transaction = ?")
}

func TestRead(t *testing.T) {
	executor := &fakeExecutor{
		readResult: map[int][]byte{
			0: []byte("hello"),
		},
	}

	config := BaseSchemaChunkerConfig.WithExecutor(executor)
	chunker := common.MustNewSQLByteChunker(config)

	data, err := chunker.ReadChunkedBytes(context.Background(), "test-key")
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), data)
}

func TestMultipleChunks(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}

	config := BaseSchemaChunkerConfig.WithExecutor(executor)
	config.MaxChunkSize = 5
	chunker := common.MustNewSQLByteChunker(config)

	createdAt := uint64(100)
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", []byte("hello world!"), createdAt)
	require.NoError(t, err)

	// UPDATE (tombstone) + INSERT
	require.Len(t, txn.capturedSQL, 2)

	// "hello world!" is 12 bytes, chunk size 5 => 3 chunks (5+5+2)
	// Each chunk has 4 args (name, chunk_index, chunk_data, created_transaction)
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 12) // 3 chunks * 4 values
}
