package spanner

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	sq "github.com/Masterminds/squirrel"

	"github.com/authzed/spicedb/internal/datastore/common"
)

const (
	// Spanner has no practical limit on BYTES column size,
	// but we use 1MB chunks for reasonable memory usage and query performance.
	spannerMaxChunkSize = 1024 * 1024 // 1MB
)

// BaseSchemaChunkerConfig provides the base configuration for Spanner schema chunking.
// Spanner uses @p-style placeholders and delete-and-insert write mode.
var BaseSchemaChunkerConfig = common.SQLByteChunkerConfig[any]{
	TableName:         tableSchema,
	NameColumn:        colSchemaName,
	ChunkIndexColumn:  colSchemaChunkIndex,
	ChunkDataColumn:   colSchemaChunkData,
	MaxChunkSize:      spannerMaxChunkSize,
	PlaceholderFormat: sq.AtP,
	WriteMode:         common.WriteModeDeleteAndInsert,
}

// spannerChunkedBytesExecutor implements common.ChunkedBytesExecutor for Spanner's mutation-based API.
type spannerChunkedBytesExecutor struct {
	rwt *spanner.ReadWriteTransaction
}

// newSpannerChunkedBytesExecutor creates a new executor for Spanner chunk operations.
func newSpannerChunkedBytesExecutor(rwt *spanner.ReadWriteTransaction) *spannerChunkedBytesExecutor {
	return &spannerChunkedBytesExecutor{rwt: rwt}
}

// BeginTransaction returns a transaction wrapper for Spanner operations.
func (e *spannerChunkedBytesExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	return &spannerChunkedBytesTransaction{rwt: e.rwt}, nil
}

// ExecuteRead executes a SELECT query and returns chunk data.
func (e *spannerChunkedBytesExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	iter := e.rwt.Query(ctx, statementFromSQL(sql, args))
	defer iter.Stop()

	chunks := make(map[int][]byte)
	if err := iter.Do(func(row *spanner.Row) error {
		var chunkIndex int64
		var chunkData []byte
		if err := row.Columns(&chunkIndex, &chunkData); err != nil {
			return err
		}
		chunks[int(chunkIndex)] = chunkData
		return nil
	}); err != nil {
		return nil, err
	}

	return chunks, nil
}

// spannerChunkedBytesTransaction implements common.ChunkedBytesTransaction for Spanner.
type spannerChunkedBytesTransaction struct {
	rwt *spanner.ReadWriteTransaction
}

// ExecuteWrite converts an INSERT builder to Spanner mutations.
func (t *spannerChunkedBytesTransaction) ExecuteWrite(ctx context.Context, builder sq.InsertBuilder) error {
	sql, args, err := builder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build insert: %w", err)
	}

	// Convert the INSERT statement to a Spanner mutation.
	// This assumes the specific format from the chunker (validated in tests).
	mutation, err := t.convertInsertToMutation(sql, args)
	if err != nil {
		return err
	}

	return t.rwt.BufferWrite([]*spanner.Mutation{mutation})
}

// ExecuteDelete converts a DELETE builder to Spanner mutations.
func (t *spannerChunkedBytesTransaction) ExecuteDelete(ctx context.Context, builder sq.DeleteBuilder) error {
	// For schema table, we can just delete all keys
	// The chunker only deletes from the schema table by name
	mutation := spanner.Delete(tableSchema, spanner.AllKeys())
	return t.rwt.BufferWrite([]*spanner.Mutation{mutation})
}

// ExecuteUpdate converts an UPDATE builder to Spanner mutations.
func (t *spannerChunkedBytesTransaction) ExecuteUpdate(ctx context.Context, builder sq.UpdateBuilder) error {
	return errors.New("ExecuteUpdate not implemented for Spanner chunked bytes")
}

// convertInsertToMutation converts an INSERT SQL statement to a Spanner mutation.
// This assumes the chunker generates INSERT statements in the expected format:
// INSERT INTO schema (name, chunk_index, chunk_data) VALUES (@p1, @p2, @p3)
// The format is validated in schema_chunker_test.go.
func (t *spannerChunkedBytesTransaction) convertInsertToMutation(sql string, args []any) (*spanner.Mutation, error) {
	// We assume the chunker provides exactly 3 args in the correct order:
	// [name, chunk_index, chunk_data]
	if len(args) != 3 {
		return nil, fmt.Errorf("expected 3 args from chunker, got %d", len(args))
	}

	// Add timestamp column
	cols := []string{colSchemaName, colSchemaChunkIndex, colSchemaChunkData, colTimestamp}
	vals := []any{args[0], args[1], args[2], spanner.CommitTimestamp}

	return spanner.Insert(tableSchema, cols, vals), nil
}

// spannerSchemaReadExecutor implements common.ChunkedBytesExecutor for read-only operations.
type spannerSchemaReadExecutor struct {
	txSource txFactory
}

// BeginTransaction returns nil since read operations don't need transactions.
func (e *spannerSchemaReadExecutor) BeginTransaction(ctx context.Context) (common.ChunkedBytesTransaction, error) {
	return nil, errors.New("BeginTransaction not supported for read-only executor")
}

// ExecuteRead executes a SELECT query and returns chunk data.
func (e *spannerSchemaReadExecutor) ExecuteRead(ctx context.Context, builder sq.SelectBuilder) (map[int][]byte, error) {
	sql, args, err := builder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	tx := e.txSource()
	iter := tx.Query(ctx, statementFromSQL(sql, args))
	defer iter.Stop()

	chunks := make(map[int][]byte)
	if err := iter.Do(func(row *spanner.Row) error {
		var chunkIndex int64
		var chunkData []byte
		if err := row.Columns(&chunkIndex, &chunkData); err != nil {
			return err
		}
		chunks[int(chunkIndex)] = chunkData
		return nil
	}); err != nil {
		return nil, err
	}

	return chunks, nil
}

var (
	_ common.ChunkedBytesExecutor    = (*spannerChunkedBytesExecutor)(nil)
	_ common.ChunkedBytesTransaction = (*spannerChunkedBytesTransaction)(nil)
	_ common.ChunkedBytesExecutor    = (*spannerSchemaReadExecutor)(nil)
)
