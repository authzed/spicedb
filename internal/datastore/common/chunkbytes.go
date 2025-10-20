package common

import (
	"context"
	"errors"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// ChunkedBytesTransaction defines the interface for executing SQL queries within a transaction.
type ChunkedBytesTransaction interface {
	// ExecuteWrite executes an INSERT query.
	ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error

	// ExecuteDelete executes a DELETE query.
	ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error

	// ExecuteUpdate executes an UPDATE query.
	ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error
}

// ChunkedBytesExecutor defines the interface for creating transactions for chunked byte operations.
type ChunkedBytesExecutor interface {
	// BeginTransaction starts a new transaction for chunked byte operations.
	BeginTransaction(ctx context.Context) (ChunkedBytesTransaction, error)

	// ExecuteRead executes a SELECT query and returns the results as a map of chunk index to chunk data.
	ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error)
}

// WriteMode defines how chunked data should be written.
type WriteMode int

const (
	// WriteModeInsertWithTombstones inserts new chunks and marks old chunks with a tombstone.
	// Requires TombstoneColumn to be set in config.
	WriteModeInsertWithTombstones WriteMode = iota

	// WriteModeDeleteAndInsert deletes all existing chunks for the key before inserting new ones.
	// Useful for replacing data completely.
	WriteModeDeleteAndInsert
)

// SQLByteChunkerConfig contains the configuration for creating a SQLByteChunker.
type SQLByteChunkerConfig[T any] struct {
	// TableName is the name of the table storing the chunked data.
	TableName string

	// NameColumn is the column name that stores the identifier for the byte data.
	NameColumn string

	// ChunkIndexColumn is the column name that stores the chunk index (0-based).
	ChunkIndexColumn string

	// ChunkDataColumn is the column name that stores the chunk bytes.
	ChunkDataColumn string

	// MaxChunkSize is the maximum size in bytes for each chunk.
	MaxChunkSize int

	// PlaceholderFormat is the placeholder format for SQL queries (e.g., sq.Question, sq.Dollar).
	PlaceholderFormat sq.PlaceholderFormat

	// Executor is the executor for running SQL queries.
	Executor ChunkedBytesExecutor

	// WriteMode defines how chunked data should be written (insert-with-tombstones or delete-and-insert).
	WriteMode WriteMode

	// CreatedAtColumn is the column name that stores when a row was created (alive timestamp/transaction ID).
	// Required when WriteMode is WriteModeInsertWithTombstones.
	CreatedAtColumn string

	// DeletedAtColumn is the column name that stores when a row was deleted (tombstone timestamp/transaction ID).
	// Required when WriteMode is WriteModeInsertWithTombstones.
	DeletedAtColumn string

	// AliveValue is the value used to indicate a row has not been deleted yet (typically max int).
	// Required when WriteMode is WriteModeInsertWithTombstones.
	AliveValue T
}

// SQLByteChunker provides methods for reading and writing byte data
// that is chunked across multiple rows in a SQL table.
type SQLByteChunker[T any] struct {
	tableName         string
	nameColumn        string
	chunkIndexColumn  string
	chunkDataColumn   string
	maxChunkSize      int
	placeholderFormat sq.PlaceholderFormat
	executor          ChunkedBytesExecutor
	writeMode         WriteMode
	createdAtColumn   string
	deletedAtColumn   string
	aliveValue        T
}

// MustNewSQLByteChunker creates a new SQLByteChunker with the specified configuration.
// Panics if the configuration is invalid.
func MustNewSQLByteChunker[T any](config SQLByteChunkerConfig[T]) *SQLByteChunker[T] {
	if config.MaxChunkSize <= 0 {
		panic("maxChunkSize must be greater than 0")
	}
	if config.TableName == "" {
		panic("tableName cannot be empty")
	}
	if config.NameColumn == "" {
		panic("nameColumn cannot be empty")
	}
	if config.ChunkIndexColumn == "" {
		panic("chunkIndexColumn cannot be empty")
	}
	if config.ChunkDataColumn == "" {
		panic("chunkDataColumn cannot be empty")
	}
	if config.PlaceholderFormat == nil {
		panic("placeholderFormat cannot be nil")
	}
	if config.Executor == nil {
		panic("executor cannot be nil")
	}
	if config.WriteMode == WriteModeInsertWithTombstones {
		if config.CreatedAtColumn == "" {
			panic("createdAtColumn is required when using WriteModeInsertWithTombstones")
		}
		if config.DeletedAtColumn == "" {
			panic("deletedAtColumn is required when using WriteModeInsertWithTombstones")
		}
	}

	return &SQLByteChunker[T]{
		tableName:         config.TableName,
		nameColumn:        config.NameColumn,
		chunkIndexColumn:  config.ChunkIndexColumn,
		chunkDataColumn:   config.ChunkDataColumn,
		maxChunkSize:      config.MaxChunkSize,
		placeholderFormat: config.PlaceholderFormat,
		executor:          config.Executor,
		writeMode:         config.WriteMode,
		createdAtColumn:   config.CreatedAtColumn,
		deletedAtColumn:   config.DeletedAtColumn,
		aliveValue:        config.AliveValue,
	}
}

// WriteChunkedBytes writes chunked byte data to the database within a transaction.
//
// Parameters:
//   - ctx: Context for the operation
//   - name: The unique identifier for this byte data
//   - data: The bytes to be chunked and stored
//   - createdAtValue: The value for the created_at column (typically a transaction ID or timestamp).
//     Required when using WriteModeInsertWithTombstones. For WriteModeDeleteAndInsert, this parameter is ignored.
func (c *SQLByteChunker[T]) WriteChunkedBytes(
	ctx context.Context,
	name string,
	data []byte,
	createdAtValue T,
) error {
	if name == "" {
		return errors.New("name cannot be empty")
	}

	// Begin transaction
	txn, err := c.executor.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Handle existing chunks based on write mode
	switch c.writeMode {
	case WriteModeDeleteAndInsert:
		// Delete all existing chunks
		deleteBuilder := sq.StatementBuilder.
			PlaceholderFormat(c.placeholderFormat).
			Delete(c.tableName).
			Where(sq.Eq{c.nameColumn: name})

		if err := txn.ExecuteDelete(ctx, deleteBuilder); err != nil {
			return fmt.Errorf("failed to delete existing chunks: %w", err)
		}
	case WriteModeInsertWithTombstones:
		// Mark existing alive chunks with tombstone
		updateBuilder := sq.StatementBuilder.
			PlaceholderFormat(c.placeholderFormat).
			Update(c.tableName).
			Set(c.deletedAtColumn, createdAtValue).
			Where(sq.Eq{c.nameColumn: name}).
			Where(sq.Eq{c.deletedAtColumn: c.aliveValue})

		if err := txn.ExecuteUpdate(ctx, updateBuilder); err != nil {
			return fmt.Errorf("failed to tombstone existing chunks: %w", err)
		}
	}

	// Build the insert query
	insertBuilder := sq.StatementBuilder.
		PlaceholderFormat(c.placeholderFormat).
		Insert(c.tableName)

	// Chunk the data
	chunks := c.chunkData(data)
	if len(chunks) == 0 {
		// Handle empty data case - insert a single empty chunk
		chunks = [][]byte{{}}
	}

	// Set up the columns - base columns plus created_at (if using tombstone mode)
	columns := []string{c.nameColumn, c.chunkIndexColumn, c.chunkDataColumn}
	if c.writeMode == WriteModeInsertWithTombstones {
		columns = append(columns, c.createdAtColumn)
	}
	insertBuilder = insertBuilder.Columns(columns...)

	// Add each chunk as a row
	for index, chunk := range chunks {
		values := []any{name, index, chunk}

		// Add created_at value if using tombstone mode (deleted_at is written automatically)
		if c.writeMode == WriteModeInsertWithTombstones {
			values = append(values, createdAtValue)
		}

		insertBuilder = insertBuilder.Values(values...)
	}

	// Execute the insert
	if err := txn.ExecuteWrite(ctx, insertBuilder); err != nil {
		return fmt.Errorf("failed to insert chunks: %w", err)
	}

	return nil
}

// DeleteChunkedBytes deletes or tombstones all chunks for a given name within a transaction.
//
// Parameters:
//   - ctx: Context for the operation
//   - name: The unique identifier for the byte data to delete
//   - deletedAtValue: The value to write to the deleted_at column (typically a transaction ID or timestamp).
//     Required when using WriteModeInsertWithTombstones. For WriteModeDeleteAndInsert, this parameter is ignored.
func (c *SQLByteChunker[T]) DeleteChunkedBytes(
	ctx context.Context,
	name string,
	deletedAtValue T,
) error {
	if name == "" {
		return errors.New("name cannot be empty")
	}

	// Begin transaction
	txn, err := c.executor.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	switch c.writeMode {
	case WriteModeDeleteAndInsert:
		// Actually delete the chunks
		deleteBuilder := sq.StatementBuilder.
			PlaceholderFormat(c.placeholderFormat).
			Delete(c.tableName).
			Where(sq.Eq{c.nameColumn: name})

		if err := txn.ExecuteDelete(ctx, deleteBuilder); err != nil {
			return fmt.Errorf("failed to delete chunks: %w", err)
		}
	case WriteModeInsertWithTombstones:
		// Mark alive chunks with tombstone by setting deleted_at column
		updateBuilder := sq.StatementBuilder.
			PlaceholderFormat(c.placeholderFormat).
			Update(c.tableName).
			Set(c.deletedAtColumn, deletedAtValue).
			Where(sq.Eq{c.nameColumn: name}).
			Where(sq.Eq{c.deletedAtColumn: c.aliveValue})

		if err := txn.ExecuteUpdate(ctx, updateBuilder); err != nil {
			return fmt.Errorf("failed to tombstone chunks: %w", err)
		}
	}

	return nil
}

// ReadChunkedBytes reads and reassembles chunked byte data from the database.
//
// Parameters:
//   - ctx: Context for the operation
//   - name: The unique identifier for the byte data to read
//
// Returns the reassembled byte data or an error if chunks are missing or invalid.
func (c *SQLByteChunker[T]) ReadChunkedBytes(
	ctx context.Context,
	name string,
) ([]byte, error) {
	if name == "" {
		return nil, errors.New("name cannot be empty")
	}

	selectBuilder := sq.StatementBuilder.
		PlaceholderFormat(c.placeholderFormat).
		Select(c.chunkIndexColumn, c.chunkDataColumn).
		From(c.tableName).
		Where(sq.Eq{c.nameColumn: name}).
		OrderBy(c.chunkIndexColumn + " ASC")

	// Execute the query
	chunks, err := c.executor.ExecuteRead(ctx, selectBuilder)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunks: %w", err)
	}

	// Reassemble the chunks
	data, err := c.reassembleChunks(chunks)
	if err != nil {
		return nil, fmt.Errorf("failed to reassemble chunks: %w", err)
	}

	return data, nil
}

// reassembleChunks takes the chunks read from the database and reassembles them
// into the original byte array. It validates that all chunks are present and in order.
func (c *SQLByteChunker[T]) reassembleChunks(chunks map[int][]byte) ([]byte, error) {
	if len(chunks) == 0 {
		return nil, errors.New("no chunks found")
	}

	// Validate that we have all chunks from 0 to N-1
	maxIndex := -1
	for index := range chunks {
		if index > maxIndex {
			maxIndex = index
		}
	}

	// Check for missing chunks
	for i := 0; i <= maxIndex; i++ {
		if _, exists := chunks[i]; !exists {
			return nil, fmt.Errorf("missing chunk at index %d", i)
		}
	}

	// Calculate total size
	totalSize := 0
	for _, chunk := range chunks {
		totalSize += len(chunk)
	}

	// Reassemble
	result := make([]byte, 0, totalSize)
	for i := 0; i <= maxIndex; i++ {
		result = append(result, chunks[i]...)
	}

	return result, nil
}

// chunkData splits the data into chunks of maxChunkSize.
func (c *SQLByteChunker[T]) chunkData(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}

	numChunks := (len(data) + c.maxChunkSize - 1) / c.maxChunkSize
	chunks := make([][]byte, 0, numChunks)

	for i := 0; i < len(data); i += c.maxChunkSize {
		end := i + c.maxChunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[i:end])
	}

	return chunks
}
