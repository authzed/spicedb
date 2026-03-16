package spanner

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
)

// fakeTransaction captures SQL and args via builder.ToSql() for verification.
type fakeTransaction struct {
	capturedSQL   []string
	capturedArgs  [][]any
	deleteQueries []string
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
	f.deleteQueries = append(f.deleteQueries, sql)
	return nil
}

func (f *fakeTransaction) ExecuteUpdate(_ context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
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
	require.Equal(t, tableSchema, BaseSchemaChunkerConfig.TableName)
	require.Equal(t, colSchemaName, BaseSchemaChunkerConfig.NameColumn)
	require.Equal(t, colSchemaChunkIndex, BaseSchemaChunkerConfig.ChunkIndexColumn)
	require.Equal(t, colSchemaChunkData, BaseSchemaChunkerConfig.ChunkDataColumn)
	require.Equal(t, 1024*1024, BaseSchemaChunkerConfig.MaxChunkSize)
	require.Equal(t, sq.AtP, BaseSchemaChunkerConfig.PlaceholderFormat)
	require.Equal(t, common.WriteModeDeleteAndInsert, BaseSchemaChunkerConfig.WriteMode)
}

func TestWrite(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}

	config := BaseSchemaChunkerConfig.WithExecutor(executor)
	chunker := common.MustNewSQLByteChunker(config)

	err := chunker.WriteChunkedBytes(context.Background(), "test-key", []byte("hello"), nil)
	require.NoError(t, err)

	// Should have DELETE + INSERT
	require.Len(t, txn.capturedSQL, 2)
	require.Len(t, txn.deleteQueries, 1)

	// DELETE uses @p placeholders
	require.Contains(t, txn.capturedSQL[0], "DELETE FROM "+tableSchema)
	require.Contains(t, txn.capturedSQL[0], "@p1")

	// INSERT uses @p placeholders
	require.Contains(t, txn.capturedSQL[1], "INSERT INTO "+tableSchema)
	require.Contains(t, txn.capturedSQL[1], "@p1")
}

func TestDelete(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}

	config := BaseSchemaChunkerConfig.WithExecutor(executor)
	chunker := common.MustNewSQLByteChunker(config)

	err := chunker.DeleteChunkedBytes(context.Background(), "test-key", nil)
	require.NoError(t, err)

	require.Len(t, txn.capturedSQL, 1)
	require.Contains(t, txn.capturedSQL[0], "DELETE FROM "+tableSchema)
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

	err := chunker.WriteChunkedBytes(context.Background(), "test-key", []byte("hello world!"), nil)
	require.NoError(t, err)

	// DELETE + INSERT
	require.Len(t, txn.capturedSQL, 2)

	// "hello world!" is 12 bytes, chunk size 5 => 3 chunks (5+5+2)
	// Each chunk has 3 args (name, chunk_index, chunk_data) for delete-and-insert mode
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 9) // 3 chunks * 3 values
}

func TestConvertInsertToMutations_SingleRow(t *testing.T) {
	txn := &spannerChunkedBytesTransaction{}

	args := []any{"schema_name", int64(0), []byte("chunk_data_0")}
	mutations, err := txn.convertInsertToMutations(args)
	require.NoError(t, err)
	require.Len(t, mutations, 1)
}

func TestConvertInsertToMutations_MultipleRows(t *testing.T) {
	txn := &spannerChunkedBytesTransaction{}

	args := []any{
		"schema_name", int64(0), []byte("chunk_0"),
		"schema_name", int64(1), []byte("chunk_1"),
		"schema_name", int64(2), []byte("chunk_2"),
	}
	mutations, err := txn.convertInsertToMutations(args)
	require.NoError(t, err)
	require.Len(t, mutations, 3)
}

func TestConvertInsertToMutations_EmptyArgs(t *testing.T) {
	txn := &spannerChunkedBytesTransaction{}

	_, err := txn.convertInsertToMutations([]any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected args in groups of 3")
}

func TestConvertInsertToMutations_InvalidArgCount(t *testing.T) {
	txn := &spannerChunkedBytesTransaction{}

	_, err := txn.convertInsertToMutations([]any{"a", "b"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected args in groups of 3")
}

// Verify the returned mutations are spanner.Mutation (non-nil).
func TestConvertInsertToMutations_MutationsAreValid(t *testing.T) {
	txn := &spannerChunkedBytesTransaction{}

	args := []any{
		"name1", int64(0), []byte("data0"),
		"name1", int64(1), []byte("data1"),
	}
	mutations, err := txn.convertInsertToMutations(args)
	require.NoError(t, err)
	require.Len(t, mutations, 2)

	for _, m := range mutations {
		require.IsType(t, &spanner.Mutation{}, m)
	}
}
