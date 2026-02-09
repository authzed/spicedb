package common

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/authzed/spicedb/internal/datastore/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	// UnifiedSchemaName is the name used to store the unified schema in the schema table.
	UnifiedSchemaName = "unified_schema"

	// LiveDeletedTxnID is the transaction ID value used to indicate a row has not been deleted yet.
	// This is the maximum value for uint64, used by databases with tombstone-based deletion.
	LiveDeletedTxnID = uint64(math.MaxInt64)
)

// SQLSingleStoreSchemaReaderWriter implements both SingleStoreSchemaReader and SingleStoreSchemaWriter
// using the SQL byte chunking system. This provides a common implementation that SQL-based datastores
// can use.
//
// The type parameter T represents the transaction ID type used by the datastore:
// - uint64 for datastores with numeric transaction IDs (MySQL, Postgres)
// - any for datastores without transaction IDs (CRDB, Spanner in delete-and-insert mode)
type SQLSingleStoreSchemaReaderWriter[T any] struct {
	chunker               *SQLByteChunker[T]
	transactionIDProvider func(ctx context.Context) T
}

// NewSQLSingleStoreSchemaReaderWriter creates a new SQL-based single-store schema reader/writer.
// The transactionIDProvider function should return the appropriate transaction ID for the current context.
// For datastores that don't use transaction IDs, this should return the zero value.
func NewSQLSingleStoreSchemaReaderWriter[T any](
	chunker *SQLByteChunker[T],
	transactionIDProvider func(ctx context.Context) T,
) *SQLSingleStoreSchemaReaderWriter[T] {
	return &SQLSingleStoreSchemaReaderWriter[T]{
		chunker:               chunker,
		transactionIDProvider: transactionIDProvider,
	}
}

// ReadStoredSchema reads the stored schema from the unified schema table.
func (s *SQLSingleStoreSchemaReaderWriter[T]) ReadStoredSchema(ctx context.Context) (*core.StoredSchema, error) {
	data, err := s.chunker.ReadChunkedBytes(ctx, UnifiedSchemaName)
	if err != nil {
		if isNoChunksFoundError(err) {
			return nil, datastore.ErrSchemaNotFound
		}
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	storedSchema, err := schema.UnmarshalStoredSchema(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return storedSchema, nil
}

// WriteStoredSchema writes the stored schema to the unified schema table.
func (s *SQLSingleStoreSchemaReaderWriter[T]) WriteStoredSchema(ctx context.Context, storedSchema *core.StoredSchema) error {
	data, err := schema.MarshalStoredSchema(storedSchema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	transactionID := s.transactionIDProvider(ctx)
	if err := s.chunker.WriteChunkedBytes(ctx, UnifiedSchemaName, data, transactionID); err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	return nil
}

// isNoChunksFoundError checks if the error indicates no chunks were found.
func isNoChunksFoundError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	// Check both the error message and wrapped errors
	return strings.Contains(errMsg, "no chunks found") || strings.Contains(errMsg, "failed to reassemble chunks")
}

var (
	_ datastore.SingleStoreSchemaReader = (*SQLSingleStoreSchemaReaderWriter[uint64])(nil)
	_ datastore.SingleStoreSchemaWriter = (*SQLSingleStoreSchemaReaderWriter[uint64])(nil)
	_ datastore.SingleStoreSchemaReader = (*SQLSingleStoreSchemaReaderWriter[any])(nil)
	_ datastore.SingleStoreSchemaWriter = (*SQLSingleStoreSchemaReaderWriter[any])(nil)
)

// NoTransactionID is a helper function that returns a zero-value transaction ID.
// This can be used for datastores that don't track transaction IDs.
func NoTransactionID[T any](_ context.Context) T {
	var zero T
	return zero
}

// StaticTransactionID returns a function that always returns the given transaction ID.
// This is useful for testing or for datastores that use a constant value.
func StaticTransactionID[T any](id T) func(context.Context) T {
	return func(_ context.Context) T {
		return id
	}
}

// NewSQLSingleStoreSchemaReaderWriterForTransactionIDs is a convenience function for creating a schema reader/writer
// that uses uint64 transaction IDs for datastores with explicit transaction ID tracking (e.g., MySQL, Postgres).
func NewSQLSingleStoreSchemaReaderWriterForTransactionIDs(
	chunker *SQLByteChunker[uint64],
	transactionIDProvider func(ctx context.Context) uint64,
) *SQLSingleStoreSchemaReaderWriter[uint64] {
	return NewSQLSingleStoreSchemaReaderWriter(chunker, transactionIDProvider)
}

// NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC is a convenience function for creating a schema reader/writer
// for datastores with built-in MVCC that don't require explicit transaction ID tracking (e.g., CRDB, Spanner).
func NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC(
	chunker *SQLByteChunker[any],
) *SQLSingleStoreSchemaReaderWriter[any] {
	return NewSQLSingleStoreSchemaReaderWriter(chunker, NoTransactionID[any])
}

// ValidateStoredSchema validates that a stored schema is well-formed.
func ValidateStoredSchema(storedSchema *core.StoredSchema) error {
	if storedSchema == nil {
		return errors.New("stored schema is nil")
	}

	if storedSchema.Version == 0 {
		return errors.New("stored schema version is 0")
	}

	v1 := storedSchema.GetV1()
	if v1 == nil {
		return fmt.Errorf("unsupported schema version: %d", storedSchema.Version)
	}

	if v1.SchemaText == "" {
		return errors.New("schema text is empty")
	}

	if v1.SchemaHash == "" {
		return errors.New("schema hash is empty")
	}

	return nil
}

// SQLSchemaReaderWriter provides a cached implementation for reading and writing schemas
// across SQL-based datastores. It caches the chunker configuration and creates temporary
// chunkers with the appropriate executors for each operation.
//
// Type parameters:
//   - T: the transaction ID type (uint64 for Postgres/MySQL, any for CRDB/Spanner)
//   - R: the revision type (must implement datastore.Revision)
type SQLSchemaReaderWriter[T any, R datastore.Revision] struct {
	chunkerConfig SQLByteChunkerConfig[T]
	cacheOptions  options.SchemaCacheOptions
	cache         *SchemaHashCache
}

// NewSQLSchemaReaderWriter creates a new SQLSchemaReaderWriter with the given chunker configuration and cache options.
// The configuration is cached and reused for all read/write operations.
func NewSQLSchemaReaderWriter[T any, R datastore.Revision](
	chunkerConfig SQLByteChunkerConfig[T],
	cacheOptions options.SchemaCacheOptions,
) (*SQLSchemaReaderWriter[T, R], error) {
	cache, err := NewSchemaHashCache(cacheOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema cache: %w", err)
	}

	return &SQLSchemaReaderWriter[T, R]{
		chunkerConfig: chunkerConfig,
		cacheOptions:  cacheOptions,
		cache:         cache,
	}, nil
}

// Close cleans up resources used by the schema reader/writer.
// No-op for hash-based cache.
func (s *SQLSchemaReaderWriter[T, R]) Close() {
	// No resources to clean up for hash-based cache
}

// ReadSchema reads the stored schema using the provided executor.
// The executor determines how the read operation is performed (e.g., with revision awareness).
// The revision and schemaHash parameters are used for cache lookup. If hash is a bypass sentinel
// (NoSchemaHashInTransaction, NoSchemaHashForTesting, or NoSchemaHashForWatch), the cache is bypassed
// for reads (but the result is still loaded).
func (s *SQLSchemaReaderWriter[T, R]) ReadSchema(ctx context.Context, executor ChunkedBytesExecutor, rev datastore.Revision, schemaHash datastore.SchemaHash) (*core.StoredSchema, error) {
	// Use GetOrLoad pattern - it handles both cache lookup and loading
	loader := func(ctx context.Context) (*core.StoredSchema, error) {
		return s.readSchemaFromDatastore(ctx, executor)
	}

	return s.cache.GetOrLoad(ctx, rev, schemaHash, loader)
}

// readSchemaFromDatastore reads the schema directly from the datastore without cache.
func (s *SQLSchemaReaderWriter[T, R]) readSchemaFromDatastore(ctx context.Context, executor ChunkedBytesExecutor) (*core.StoredSchema, error) {
	// Create a temporary chunker with the provided executor
	chunker := MustNewSQLByteChunker(s.chunkerConfig.WithExecutor(executor))

	// Read and reassemble the schema chunks
	data, err := chunker.ReadChunkedBytes(ctx, UnifiedSchemaName)
	if err != nil {
		if isNoChunksFoundError(err) {
			return nil, datastore.ErrSchemaNotFound
		}
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	// Unmarshal the stored schema
	storedSchema := &core.StoredSchema{}
	if err := storedSchema.UnmarshalVT(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return storedSchema, nil
}

// WriteSchema writes the stored schema using the provided executor and transaction ID provider.
// The executor determines how the write operation is performed (e.g., within a transaction).
// The transactionIDProvider returns the transaction ID to use for tombstone-based datastores.
func (s *SQLSchemaReaderWriter[T, R]) WriteSchema(ctx context.Context, schema *core.StoredSchema, executor ChunkedBytesExecutor, transactionIDProvider func(ctx context.Context) T) error {
	if schema == nil {
		return errors.New("stored schema cannot be nil")
	}

	if schema.Version == 0 {
		return errors.New("stored schema version cannot be 0")
	}

	// Create a temporary chunker with the provided executor
	chunker := MustNewSQLByteChunker(s.chunkerConfig.WithExecutor(executor))

	// Marshal the schema
	data, err := schema.MarshalVT()
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	// Get the transaction ID (if applicable)
	transactionID := transactionIDProvider(ctx)

	// Write the schema chunks
	if err := chunker.WriteChunkedBytes(ctx, UnifiedSchemaName, data, transactionID); err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	return nil
}
