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
}

func (m *fakeExecutor) BeginTransaction(ctx context.Context) (ChunkedBytesTransaction, error) {
	return m.transaction, nil
}

func (m *fakeExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	return m.readResult, nil
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
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", data, createdAt)
	require.NoError(t, err)

	// Should have 1 UPDATE (tombstone old) + 1 INSERT query
	require.Len(t, txn.capturedSQL, 2)
	require.Len(t, txn.updateQueries, 1)
	require.Len(t, txn.deleteQueries, 0)

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
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", data, 0)
	require.NoError(t, err)

	// Should have 1 DELETE + 1 INSERT query
	require.Len(t, txn.capturedSQL, 2)
	require.Len(t, txn.deleteQueries, 1)
	require.Len(t, txn.updateQueries, 0)

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
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", data, 0)
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

	err := chunker.WriteChunkedBytes(context.Background(), "", []byte("test"), 0)
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
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", []byte{}, 0)
	require.NoError(t, err)

	// Verify that a single empty chunk was inserted
	require.Len(t, txn.capturedSQL, 2) // DELETE + INSERT
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 3) // 1 chunk * 3 values per chunk
	require.Equal(t, "test-key", insertArgs[0])
	require.Equal(t, 0, insertArgs[1])
	require.Equal(t, []byte{}, insertArgs[2])

	// Read it back
	data, err := chunker.ReadChunkedBytes(context.Background(), "test-key")
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Len(t, data, 0)
	require.Equal(t, []byte{}, data)
}

func TestReadChunkedBytes(t *testing.T) {
	tests := []struct {
		name          string
		chunks        map[int][]byte
		expectedData  []byte
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
			name:          "no chunks",
			chunks:        map[int][]byte{},
			expectedError: "no chunks found",
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

			data, err := chunker.ReadChunkedBytes(context.Background(), "test-key")

			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
			} else {
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

	_, err := chunker.ReadChunkedBytes(context.Background(), "")
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

	_, err := chunker.ReadChunkedBytes(context.Background(), "test-key")
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

	err := chunker.DeleteChunkedBytes(context.Background(), "test-key", 0)
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
	err := chunker.DeleteChunkedBytes(context.Background(), "test-key", deletedAt)
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

	err := chunker.DeleteChunkedBytes(context.Background(), "", 0)
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
				var reassembled []byte
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

	err := chunker.WriteChunkedBytes(context.Background(), "test-key", data, 0)
	require.NoError(t, err)

	// Should have 1 DELETE + 1 INSERT
	// INSERT should have 11 chunks (1024 / 100 = 10.24, rounds up to 11)
	require.Len(t, txn.capturedSQL, 2)
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 33) // 11 chunks * 3 values per chunk
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
	err := chunker.WriteChunkedBytes(context.Background(), "test-key", data, createdAt)
	require.NoError(t, err)

	// Should have 1 UPDATE (tombstone) + 1 INSERT
	// INSERT should have 11 chunks (1024 / 100 = 10.24, rounds up to 11)
	require.Len(t, txn.capturedSQL, 2)
	insertArgs := txn.capturedArgs[1]
	require.Len(t, insertArgs, 44) // 11 chunks * 4 values per chunk (name, index, data, created_at)
}
