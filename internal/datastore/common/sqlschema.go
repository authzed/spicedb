package common

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/authzed/spicedb/pkg/datastore"
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
func (s *SQLSingleStoreSchemaReaderWriter[T]) ReadStoredSchema(ctx context.Context) (*datastore.ReadOnlyStoredSchema, error) {
	data, err := s.chunker.ReadChunkedBytes(ctx, UnifiedSchemaName)
	if err != nil {
		if isNoChunksFoundError(err) {
			return nil, datastore.ErrSchemaNotFound
		}
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	storedSchema := &core.StoredSchema{}
	if err := storedSchema.UnmarshalVT(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return datastore.NewReadOnlyStoredSchema(storedSchema), nil
}

// WriteStoredSchema writes the stored schema to the unified schema table.
func (s *SQLSingleStoreSchemaReaderWriter[T]) WriteStoredSchema(ctx context.Context, storedSchema *core.StoredSchema) error {
	data, err := storedSchema.MarshalVT()
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
	return errors.Is(err, ErrNoChunksFound)
}

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
