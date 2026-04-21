package common

import (
	"context"
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
)

// fakeTransaction is a fake implementation of ChunkedBytesTransaction for testing.
type fakeTransaction struct {
	writeErr      error
	deleteErr     error
	updateErr     error
	capturedSQL   []string
	capturedArgs  [][]any
	deleteQueries []string
	updateQueries []string
}

func (f *fakeTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
	return f.writeErr
}

func (f *fakeTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
	f.deleteQueries = append(f.deleteQueries, sql)
	return f.deleteErr
}

func (f *fakeTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	f.capturedSQL = append(f.capturedSQL, sql)
	f.capturedArgs = append(f.capturedArgs, args)
	f.updateQueries = append(f.updateQueries, sql)
	return f.updateErr
}

// fakeExecutor is a fake implementation of ChunkedBytesExecutor for testing.
type fakeExecutor struct {
	readResult  map[int][]byte
	readErr     error
	transaction *fakeTransaction
	onRead      func() // Optional callback invoked on each read
}

func (m *fakeExecutor) BeginTransaction(ctx context.Context) (ChunkedBytesTransaction, error) {
	return m.transaction, nil
}

func (m *fakeExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	if m.onRead != nil {
		m.onRead()
	}
	if m.readErr != nil {
		return nil, m.readErr
	}
	return m.readResult, nil
}

// errorBeginExecutor always returns an error from BeginTransaction.
type errorBeginExecutor struct {
	err error
}

func (e *errorBeginExecutor) BeginTransaction(ctx context.Context) (ChunkedBytesTransaction, error) {
	return nil, e.err
}

func (e *errorBeginExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	return nil, e.err
}

func TestMustNewSQLByteChunker(t *testing.T) {
	tests := []struct {
		name        string
		config      SQLByteChunkerConfig[uint64]
		shouldPanic bool
	}{
		{
			name: "valid config - DeleteAndInsert mode",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: sq.Question,
				Executor:          &fakeExecutor{},
				WriteMode:         WriteModeDeleteAndInsert,
			},
			shouldPanic: false,
		},
		{
			name: "valid config - InsertWithTombstones mode",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: sq.Question,
				Executor:          &fakeExecutor{},
				WriteMode:         WriteModeInsertWithTombstones,
				CreatedAtColumn:   "created_at",
				DeletedAtColumn:   "deleted_at",
				AliveValue:        ^uint64(0), // max uint64
			},
			shouldPanic: false,
		},
		{
			name: "invalid max chunk size",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      0,
				PlaceholderFormat: sq.Question,
				Executor:          &fakeExecutor{},
			},
			shouldPanic: true,
		},
		{
			name: "empty table name",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: sq.Question,
				Executor:          &fakeExecutor{},
			},
			shouldPanic: true,
		},
		{
			name: "nil placeholder format",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: nil,
				Executor:          &fakeExecutor{},
			},
			shouldPanic: true,
		},
		{
			name: "nil executor",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: sq.Question,
				Executor:          nil,
			},
			shouldPanic: true,
		},
		{
			name: "InsertWithTombstones without created_at column",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: sq.Question,
				Executor:          &fakeExecutor{},
				WriteMode:         WriteModeInsertWithTombstones,
				DeletedAtColumn:   "deleted_at",
			},
			shouldPanic: true,
		},
		{
			name: "InsertWithTombstones without deleted_at column",
			config: SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024,
				PlaceholderFormat: sq.Question,
				Executor:          &fakeExecutor{},
				WriteMode:         WriteModeInsertWithTombstones,
				CreatedAtColumn:   "created_at",
			},
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.shouldPanic {
				require.Panics(t, func() {
					MustNewSQLByteChunker(tt.config)
				})
			} else {
				require.NotPanics(t, func() {
					chunker := MustNewSQLByteChunker(tt.config)
					require.NotNil(t, chunker)
				})
			}
		})
	}
}

func TestWriteChunkedBytes_InsertWithTombstones(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeInsertWithTombstones,
		CreatedAtColumn:   "created_at",
		DeletedAtColumn:   "deleted_at",
		AliveValue:        ^uint64(0), // max uint64
	})

	data := []byte("Hello, World! This is a test.")
	createdAt := uint64(123)
	err := chunker.WriteChunkedBytes(t.Context(), "test-key", data, createdAt)
	require.NoError(t, err)

	// Should have 1 UPDATE (tombstone old) + 1 INSERT query
	require.Len(t, txn.capturedSQL, 2)
	require.Len(t, txn.updateQueries, 1)
	require.Empty(t, txn.deleteQueries)

	// Check the UPDATE query (tombstone old chunks that are alive)
	updateSQL := txn.capturedSQL[0]
	require.Contains(t, updateSQL, "UPDATE test_table")
	require.Contains(t, updateSQL, "SET deleted_at = ?")
	require.Contains(t, updateSQL, "WHERE name = ?")
	require.Contains(t, updateSQL, "AND deleted_at = ?")
	require.Equal(t, []any{createdAt, "test-key", ^uint64(0)}, txn.capturedArgs[0])

	// Check the INSERT query
	insertSQL := txn.capturedSQL[1]
	require.Contains(t, insertSQL, "INSERT INTO test_table")
	require.Contains(t, insertSQL, "name")
	require.Contains(t, insertSQL, "chunk_index")
	require.Contains(t, insertSQL, "chunk_data")
	require.Contains(t, insertSQL, "created_at")

	// Check that we have the right number of chunks (30 bytes / 10 = 3 chunks)
	// Each chunk has 4 values: name, chunk_index, chunk_data, created_at
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 12) // 3 chunks * 4 values per chunk

	// Verify created_at values
	require.Equal(t, "test-key", insertArgs[0]) // First chunk name
	require.Equal(t, 0, insertArgs[1])          // First chunk index
	require.Equal(t, createdAt, insertArgs[3])  // First chunk created_at
	require.Equal(t, createdAt, insertArgs[7])  // Second chunk created_at
	require.Equal(t, createdAt, insertArgs[11]) // Third chunk created_at
}

func TestWriteChunkedBytes_DeleteAndInsert(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	data := []byte("Hello, World!")
	err := chunker.WriteChunkedBytes(t.Context(), "test-key", data, 0)
	require.NoError(t, err)

	// Should have 1 DELETE + 1 INSERT query
	require.Len(t, txn.capturedSQL, 2)
	require.Len(t, txn.deleteQueries, 1)
	require.Empty(t, txn.updateQueries)

	// Check the DELETE query
	deleteSQL := txn.capturedSQL[0]
	require.Contains(t, deleteSQL, "DELETE FROM test_table")
	require.Contains(t, deleteSQL, "name = ?")
	require.Equal(t, []any{"test-key"}, txn.capturedArgs[0])

	// Check the INSERT query
	insertSQL := txn.capturedSQL[1]
	require.Contains(t, insertSQL, "INSERT INTO test_table")
	require.Contains(t, insertSQL, "(name,chunk_index,chunk_data)")

	// Should have 2 chunks (13 bytes / 10 = 1.3 rounds up to 2)
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 6) // 2 chunks * 3 values per chunk
}

func TestWriteChunkedBytes_EmptyData(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	data := []byte{}
	err := chunker.WriteChunkedBytes(t.Context(), "test-key", data, 0)
	require.NoError(t, err)

	// Should insert a single empty chunk
	require.Len(t, txn.capturedSQL, 2) // DELETE + INSERT
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 3) // 1 chunk * 3 values per chunk
	require.Equal(t, "test-key", insertArgs[0])
	require.Equal(t, 0, insertArgs[1])
	require.Equal(t, []byte{}, insertArgs[2])
}

func TestWriteChunkedBytes_EmptyName(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	err := chunker.WriteChunkedBytes(t.Context(), "", []byte("test"), 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "name cannot be empty")
}

func TestWriteAndReadZeroByteChunk(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{
		transaction: txn,
		readResult: map[int][]byte{
			0: {}, // Zero-byte chunk from SQL
		},
	}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	// Write zero-byte data
	err := chunker.WriteChunkedBytes(t.Context(), "test-key", []byte{}, 0)
	require.NoError(t, err)

	// Verify that a single empty chunk was inserted
	require.Len(t, txn.capturedSQL, 2) // DELETE + INSERT
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 3) // 1 chunk * 3 values per chunk
	require.Equal(t, "test-key", insertArgs[0])
	require.Equal(t, 0, insertArgs[1])
	require.Equal(t, []byte{}, insertArgs[2])

	// Read it back
	data, err := chunker.ReadChunkedBytes(t.Context(), "test-key")
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Empty(t, data)
	require.Equal(t, []byte{}, data)
}

func TestReadChunkedBytes(t *testing.T) {
	tests := []struct {
		name          string
		chunks        map[int][]byte
		expectedData  []byte
		expectedErr   error
		expectedError string
	}{
		{
			name: "single chunk",
			chunks: map[int][]byte{
				0: []byte("Hello, World!"),
			},
			expectedData: []byte("Hello, World!"),
		},
		{
			name: "multiple chunks",
			chunks: map[int][]byte{
				0: []byte("Hello, "),
				1: []byte("World! "),
				2: []byte("Test."),
			},
			expectedData: []byte("Hello, World! Test."),
		},
		{
			name: "empty chunk",
			chunks: map[int][]byte{
				0: {},
			},
			expectedData: []byte{},
		},
		{
			name:        "no chunks",
			chunks:      map[int][]byte{},
			expectedErr: ErrNoChunksFound,
		},
		{
			name: "missing chunk in sequence",
			chunks: map[int][]byte{
				0: []byte("Hello"),
				2: []byte("World"),
			},
			expectedError: "missing chunk at index 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &fakeExecutor{
				readResult: tt.chunks,
			}
			chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
				TableName:         "test_table",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      10,
				PlaceholderFormat: sq.Question,
				Executor:          executor,
				WriteMode:         WriteModeDeleteAndInsert,
			})

			data, err := chunker.ReadChunkedBytes(t.Context(), "test-key")

			switch {
			case tt.expectedErr != nil:
				require.ErrorIs(t, err, tt.expectedErr)
			case tt.expectedError != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			default:
				require.NoError(t, err)
				require.Equal(t, tt.expectedData, data)
			}
		})
	}
}

func TestReadChunkedBytes_EmptyName(t *testing.T) {
	executor := &fakeExecutor{}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	_, err := chunker.ReadChunkedBytes(t.Context(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "name cannot be empty")
}

func TestReadChunkedBytes_ExecutorError(t *testing.T) {
	executor := &fakeExecutor{
		readErr: fmt.Errorf("database error"),
	}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	_, err := chunker.ReadChunkedBytes(t.Context(), "test-key")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read chunks")
	require.Contains(t, err.Error(), "database error")
}

func TestDeleteChunkedBytes_DeleteAndInsert(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	err := chunker.DeleteChunkedBytes(t.Context(), "test-key", 0)
	require.NoError(t, err)

	// Verify the DELETE query
	require.Len(t, txn.capturedSQL, 1)
	deleteSQL := txn.capturedSQL[0]
	require.Contains(t, deleteSQL, "DELETE FROM test_table")
	require.Contains(t, deleteSQL, "WHERE name = ?")
	require.Equal(t, []any{"test-key"}, txn.capturedArgs[0])
}

func TestDeleteChunkedBytes_InsertWithTombstones(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeInsertWithTombstones,
		CreatedAtColumn:   "created_at",
		DeletedAtColumn:   "deleted_at",
		AliveValue:        ^uint64(0), // max uint64
	})

	deletedAt := uint64(456)
	err := chunker.DeleteChunkedBytes(t.Context(), "test-key", deletedAt)
	require.NoError(t, err)

	// Verify the UPDATE query (tombstone)
	require.Len(t, txn.capturedSQL, 1)
	updateSQL := txn.capturedSQL[0]
	require.Contains(t, updateSQL, "UPDATE test_table")
	require.Contains(t, updateSQL, "SET deleted_at = ?")
	require.Contains(t, updateSQL, "WHERE name = ?")
	require.Contains(t, updateSQL, "AND deleted_at = ?")
	require.Equal(t, []any{deletedAt, "test-key", ^uint64(0)}, txn.capturedArgs[0])
}

func TestDeleteChunkedBytes_EmptyName(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	err := chunker.DeleteChunkedBytes(t.Context(), "", 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "name cannot be empty")
}

func TestChunkData(t *testing.T) {
	executor := &fakeExecutor{}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      5,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	tests := []struct {
		name          string
		data          []byte
		expectedCount int
		expectedSizes []int
	}{
		{
			name:          "empty data",
			data:          []byte{},
			expectedCount: 0,
			expectedSizes: []int{},
		},
		{
			name:          "exact chunk size",
			data:          []byte("12345"),
			expectedCount: 1,
			expectedSizes: []int{5},
		},
		{
			name:          "multiple exact chunks",
			data:          []byte("1234567890"),
			expectedCount: 2,
			expectedSizes: []int{5, 5},
		},
		{
			name:          "partial last chunk",
			data:          []byte("1234567"),
			expectedCount: 2,
			expectedSizes: []int{5, 2},
		},
		{
			name:          "single byte",
			data:          []byte("1"),
			expectedCount: 1,
			expectedSizes: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chunks := chunker.chunkData(tt.data)
			require.Len(t, chunks, tt.expectedCount)

			for i, expectedSize := range tt.expectedSizes {
				require.Len(t, chunks[i], expectedSize)
			}

			// Verify that reassembling gives us the original data
			if tt.expectedCount > 0 {
				var reassembled []byte //nolint: prealloc  // it's hard to know the combined length here
				for _, chunk := range chunks {
					reassembled = append(reassembled, chunk...)
				}
				require.Equal(t, tt.data, reassembled)
			}
		})
	}
}

func TestWriteChunkedBytes_LargeData_DeleteAndInsert(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      100,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	// Create 1KB of data
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	err := chunker.WriteChunkedBytes(t.Context(), "test-key", data, 0)
	require.NoError(t, err)

	// Should have 1 DELETE + 1 INSERT
	// INSERT should have 11 chunks (1024 / 100 = 10.24, rounds up to 11)
	require.Len(t, txn.capturedSQL, 2)
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 33) // 11 chunks * 3 values per chunk
}

func TestWithExecutor(t *testing.T) {
	executor1 := &fakeExecutor{}
	executor2 := &fakeExecutor{}

	config := SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      1024,
		PlaceholderFormat: sq.Question,
		Executor:          executor1,
		WriteMode:         WriteModeDeleteAndInsert,
	}

	// WithExecutor should return a copy with the new executor.
	newConfig := config.WithExecutor(executor2)
	require.Equal(t, executor2, newConfig.Executor)

	// Original should be unchanged.
	require.Equal(t, executor1, config.Executor)
}

func TestWithTableName(t *testing.T) {
	config := SQLByteChunkerConfig[uint64]{
		TableName:         "original_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      1024,
		PlaceholderFormat: sq.Question,
		Executor:          &fakeExecutor{},
		WriteMode:         WriteModeDeleteAndInsert,
	}

	// WithTableName should return a copy with the new table name.
	newConfig := config.WithTableName("new_table")
	require.Equal(t, "new_table", newConfig.TableName)

	// Original should be unchanged.
	require.Equal(t, "original_table", config.TableName)
}

func TestNewSQLByteChunker_EmptyColumnNames(t *testing.T) {
	base := SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      1024,
		PlaceholderFormat: sq.Question,
		Executor:          &fakeExecutor{},
		WriteMode:         WriteModeDeleteAndInsert,
	}

	t.Run("empty name column", func(t *testing.T) {
		c := base
		c.NameColumn = ""
		_, err := NewSQLByteChunker(c)
		require.ErrorContains(t, err, "nameColumn cannot be empty")
	})

	t.Run("empty chunk index column", func(t *testing.T) {
		c := base
		c.ChunkIndexColumn = ""
		_, err := NewSQLByteChunker(c)
		require.ErrorContains(t, err, "chunkIndexColumn cannot be empty")
	})

	t.Run("empty chunk data column", func(t *testing.T) {
		c := base
		c.ChunkDataColumn = ""
		_, err := NewSQLByteChunker(c)
		require.ErrorContains(t, err, "chunkDataColumn cannot be empty")
	})
}

func TestWriteChunkedBytes_BeginTransactionError(t *testing.T) {
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor: &errorBeginExecutor{
			err: fmt.Errorf("begin failed"),
		},
		WriteMode: WriteModeDeleteAndInsert,
	})

	err := chunker.WriteChunkedBytes(t.Context(), "test-key", []byte("hello"), 0)
	require.ErrorContains(t, err, "failed to begin transaction")
}

func TestWriteChunkedBytes_DeleteError(t *testing.T) {
	txn := &fakeTransaction{deleteErr: fmt.Errorf("delete failed")}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	err := chunker.WriteChunkedBytes(t.Context(), "test-key", []byte("hello"), 0)
	require.ErrorContains(t, err, "failed to delete existing chunks")
}

func TestWriteChunkedBytes_TombstoneError(t *testing.T) {
	txn := &fakeTransaction{updateErr: fmt.Errorf("update failed")}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeInsertWithTombstones,
		CreatedAtColumn:   "created_at",
		DeletedAtColumn:   "deleted_at",
		AliveValue:        ^uint64(0),
	})

	err := chunker.WriteChunkedBytes(t.Context(), "test-key", []byte("hello"), uint64(1))
	require.ErrorContains(t, err, "failed to tombstone existing chunks")
}

func TestWriteChunkedBytes_InsertError(t *testing.T) {
	txn := &fakeTransaction{writeErr: fmt.Errorf("insert failed")}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	err := chunker.WriteChunkedBytes(t.Context(), "test-key", []byte("hello"), 0)
	require.ErrorContains(t, err, "failed to insert chunks")
}

func TestDeleteChunkedBytes_BeginTransactionError(t *testing.T) {
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor: &errorBeginExecutor{
			err: fmt.Errorf("begin failed"),
		},
		WriteMode: WriteModeDeleteAndInsert,
	})

	err := chunker.DeleteChunkedBytes(t.Context(), "test-key", 0)
	require.ErrorContains(t, err, "failed to begin transaction")
}

func TestDeleteChunkedBytes_DeleteError(t *testing.T) {
	txn := &fakeTransaction{deleteErr: fmt.Errorf("delete failed")}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	err := chunker.DeleteChunkedBytes(t.Context(), "test-key", 0)
	require.ErrorContains(t, err, "failed to delete chunks")
}

func TestDeleteChunkedBytes_TombstoneError(t *testing.T) {
	txn := &fakeTransaction{updateErr: fmt.Errorf("update failed")}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      10,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeInsertWithTombstones,
		CreatedAtColumn:   "created_at",
		DeletedAtColumn:   "deleted_at",
		AliveValue:        ^uint64(0),
	})

	err := chunker.DeleteChunkedBytes(t.Context(), "test-key", uint64(1))
	require.ErrorContains(t, err, "failed to tombstone chunks")
}

func TestWriteChunkedBytes_LargeData_InsertWithTombstones(t *testing.T) {
	txn := &fakeTransaction{}
	executor := &fakeExecutor{transaction: txn}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "test_table",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      100,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeInsertWithTombstones,
		CreatedAtColumn:   "created_at",
		DeletedAtColumn:   "deleted_at",
		AliveValue:        ^uint64(0),
	})

	// Create 1KB of data
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	createdAt := uint64(999)
	err := chunker.WriteChunkedBytes(t.Context(), "test-key", data, createdAt)
	require.NoError(t, err)

	// Should have 1 UPDATE (tombstone) + 1 INSERT
	// INSERT should have 11 chunks (1024 / 100 = 10.24, rounds up to 11)
	require.Len(t, txn.capturedSQL, 2)
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 44) // 11 chunks * 4 values per chunk (name, index, data, created_at)
}
