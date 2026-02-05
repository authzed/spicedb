package common

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/schema"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestSQLSingleStoreSchemaReaderWriter_WriteAndRead(t *testing.T) {
	tests := []struct {
		name           string
		schemaText     string
		namespaces     map[string]*core.NamespaceDefinition
		caveats        map[string]*core.CaveatDefinition
		useTransaction bool
	}{
		{
			name:       "simple schema",
			schemaText: "definition user {}",
			namespaces: map[string]*core.NamespaceDefinition{
				"user": {Name: "user"},
			},
			caveats:        map[string]*core.CaveatDefinition{},
			useTransaction: true,
		},
		{
			name:       "schema with caveat",
			schemaText: "caveat is_allowed(allowed bool) { allowed }\ndefinition user {}",
			namespaces: map[string]*core.NamespaceDefinition{
				"user": {Name: "user"},
			},
			caveats: map[string]*core.CaveatDefinition{
				"is_allowed": {Name: "is_allowed"},
			},
			useTransaction: true,
		},
		{
			name:           "empty schema",
			schemaText:     "",
			namespaces:     map[string]*core.NamespaceDefinition{},
			caveats:        map[string]*core.CaveatDefinition{},
			useTransaction: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build stored schema
			storedSchema := &core.StoredSchema{
				Version: 1,
				VersionOneof: &core.StoredSchema_V1{
					V1: &core.StoredSchema_V1StoredSchema{
						SchemaText:           tt.schemaText,
						SchemaHash:           "test-hash",
						NamespaceDefinitions: tt.namespaces,
						CaveatDefinitions:    tt.caveats,
					},
				},
			}

			// Marshal the schema to simulate what would be written
			expectedData, err := proto.Marshal(storedSchema)
			require.NoError(t, err)

			// Create chunker with fake executor
			txn := &fakeTransaction{}
			executor := &fakeExecutor{
				transaction: txn,
				readResult:  map[int][]byte{0: expectedData}, // Return the expected data
			}

			chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
				TableName:         "schema",
				NameColumn:        "name",
				ChunkIndexColumn:  "chunk_index",
				ChunkDataColumn:   "chunk_data",
				MaxChunkSize:      1024 * 64,
				PlaceholderFormat: sq.Question,
				Executor:          executor,
				WriteMode:         WriteModeInsertWithTombstones,
				CreatedAtColumn:   "created_at",
				DeletedAtColumn:   "deleted_at",
				AliveValue:        999,
			})

			transactionIDProvider := func(ctx context.Context) uint64 {
				if tt.useTransaction {
					return 100
				}
				return 0
			}

			readerWriter := NewSQLSingleStoreSchemaReaderWriter(chunker, transactionIDProvider)

			// Write schema
			ctx := context.Background()
			err = readerWriter.WriteStoredSchema(ctx, storedSchema)
			require.NoError(t, err)

			// Verify write was called
			require.NotEmpty(t, txn.capturedSQL)

			// Read schema back
			readSchema, err := readerWriter.ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.NotNil(t, readSchema)

			// Verify
			require.Equal(t, storedSchema.Version, readSchema.Version)
			require.NotNil(t, readSchema.GetV1())
			require.Equal(t, tt.schemaText, readSchema.GetV1().SchemaText)
			require.Equal(t, "test-hash", readSchema.GetV1().SchemaHash)
			require.Len(t, readSchema.GetV1().NamespaceDefinitions, len(tt.namespaces))
			require.Len(t, readSchema.GetV1().CaveatDefinitions, len(tt.caveats))
		})
	}
}

func TestSQLSingleStoreSchemaReaderWriter_ReadNotFound(t *testing.T) {
	// Create chunker with fake executor that returns empty result
	executor := &fakeExecutor{
		readResult: map[int][]byte{}, // No chunks found
	}
	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "schema",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      1024 * 64,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	readerWriter := NewSQLSingleStoreSchemaReaderWriter(chunker, StaticTransactionID[uint64](0))

	// Try to read non-existent schema
	ctx := context.Background()
	_, err := readerWriter.ReadStoredSchema(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, datastore.ErrSchemaNotFound)
}

func TestSQLSingleStoreSchemaReaderWriter_WithBuiltInMVCC(t *testing.T) {
	// Test with "any" type parameter for datastores with built-in MVCC (like CRDB/Spanner)
	storedSchema := &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText:           "definition user {}",
				SchemaHash:           "test-hash",
				NamespaceDefinitions: map[string]*core.NamespaceDefinition{"user": {Name: "user"}},
				CaveatDefinitions:    map[string]*core.CaveatDefinition{},
			},
		},
	}

	expectedData, err := storedSchema.MarshalVT()
	require.NoError(t, err)

	txn := &fakeTransaction{}
	executor := &fakeExecutor{
		transaction: txn,
		readResult:  map[int][]byte{0: expectedData},
	}

	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[any]{
		TableName:         "schema",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      1024 * 64,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeDeleteAndInsert,
	})

	readerWriter := NewSQLSingleStoreSchemaReaderWriterWithBuiltInMVCC(chunker)

	// Write schema
	ctx := context.Background()
	err = readerWriter.WriteStoredSchema(ctx, storedSchema)
	require.NoError(t, err)

	// Read schema back
	readSchema, err := readerWriter.ReadStoredSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, readSchema)
	require.Equal(t, "definition user {}", readSchema.GetV1().SchemaText)
}

func TestValidateStoredSchema(t *testing.T) {
	tests := []struct {
		name        string
		schema      *core.StoredSchema
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil schema",
			schema:      nil,
			expectError: true,
			errorMsg:    "stored schema is nil",
		},
		{
			name: "zero version",
			schema: &core.StoredSchema{
				Version: 0,
			},
			expectError: true,
			errorMsg:    "stored schema version is 0",
		},
		{
			name: "missing v1",
			schema: &core.StoredSchema{
				Version: 1,
			},
			expectError: true,
			errorMsg:    "unsupported schema version",
		},
		{
			name: "empty schema text",
			schema: &core.StoredSchema{
				Version: 1,
				VersionOneof: &core.StoredSchema_V1{
					V1: &core.StoredSchema_V1StoredSchema{
						SchemaText: "",
						SchemaHash: "hash",
					},
				},
			},
			expectError: true,
			errorMsg:    "schema text is empty",
		},
		{
			name: "empty schema hash",
			schema: &core.StoredSchema{
				Version: 1,
				VersionOneof: &core.StoredSchema_V1{
					V1: &core.StoredSchema_V1StoredSchema{
						SchemaText: "definition user {}",
						SchemaHash: "",
					},
				},
			},
			expectError: true,
			errorMsg:    "schema hash is empty",
		},
		{
			name: "valid schema",
			schema: &core.StoredSchema{
				Version: 1,
				VersionOneof: &core.StoredSchema_V1{
					V1: &core.StoredSchema_V1StoredSchema{
						SchemaText:           "definition user {}",
						SchemaHash:           "hash",
						NamespaceDefinitions: map[string]*core.NamespaceDefinition{},
						CaveatDefinitions:    map[string]*core.CaveatDefinition{},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateStoredSchema(tt.schema)
			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildAndMarshal(t *testing.T) {
	definitions := []datastore.SchemaDefinition{
		&core.NamespaceDefinition{Name: "user"},
		&core.CaveatDefinition{Name: "is_allowed"},
	}
	schemaText := "definition user {}\ncaveat is_allowed(allowed bool) { allowed }"

	// Build stored schema
	storedSchema, err := schema.BuildStoredSchemaFromDefinitions(definitions, schemaText)
	require.NoError(t, err)
	require.NotNil(t, storedSchema)
	require.Equal(t, uint32(1), storedSchema.Version)

	v1 := storedSchema.GetV1()
	require.NotNil(t, v1)
	require.Equal(t, schemaText, v1.SchemaText)
	require.Len(t, v1.NamespaceDefinitions, 1)
	require.Len(t, v1.CaveatDefinitions, 1)

	// Marshal
	data, err := schema.MarshalStoredSchema(storedSchema)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal
	unmarshaled, err := schema.UnmarshalStoredSchema(data)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled)
	require.True(t, storedSchema.EqualVT(unmarshaled))
}

func TestHelperFunctions(t *testing.T) {
	// Test NoTransactionID
	ctx := context.Background()
	result := NoTransactionID[uint64](ctx)
	require.Equal(t, uint64(0), result)

	// Test StaticTransactionID
	staticFunc := StaticTransactionID[uint64](12345)
	require.Equal(t, uint64(12345), staticFunc(ctx))

	// Test with any type
	anyResult := NoTransactionID[any](ctx)
	require.Nil(t, anyResult)
}

func TestSQLSchemaReaderWriter_Singleflight(t *testing.T) {
	t.Run("uses singleflight for revisioned reads", func(t *testing.T) {
		// Create a schema to return
		storedSchema := &core.StoredSchema{
			Version: 1,
			VersionOneof: &core.StoredSchema_V1{
				V1: &core.StoredSchema_V1StoredSchema{
					SchemaText: "definition user {}",
					SchemaHash: "hash1",
				},
			},
		}
		data, err := storedSchema.MarshalVT()
		require.NoError(t, err)

		// Create executor that counts reads (use atomic for thread safety)
		var readCount atomic.Int32
		executor := &fakeExecutor{
			readResult: map[int][]byte{0: data},
			onRead: func() {
				readCount.Add(1)
			},
		}

		// Create config and schema reader/writer (cache disabled to test singleflight only)
		config := SQLByteChunkerConfig[uint64]{
			TableName:         "schema",
			NameColumn:        "name",
			ChunkIndexColumn:  "chunk_index",
			ChunkDataColumn:   "chunk_data",
			MaxChunkSize:      1024 * 64,
			PlaceholderFormat: sq.Question,
			Executor:          executor,
			WriteMode:         WriteModeDeleteAndInsert,
		}

		schemaRW := NewSQLSchemaReaderWriter[uint64, testRevision](
			config,
			options.SchemaCacheOptions{
				MaximumCacheMemoryBytes: 0, // Disable cache to test pure singleflight
			},
		)

		// Make 10 concurrent reads for the same revision
		const numReads = 10
		rev := testRevision{id: 123}

		type result struct {
			schema *core.StoredSchema
			err    error
		}
		results := make(chan result, numReads)

		// Use WaitGroup to ensure all goroutines start at the same time
		var ready sync.WaitGroup
		ready.Add(numReads)
		var start sync.WaitGroup
		start.Add(1)

		for i := 0; i < numReads; i++ {
			go func() {
				ready.Done()
				start.Wait() // Wait for all goroutines to be ready
				schema, err := schemaRW.ReadSchema(context.Background(), executor, &rev)
				results <- result{schema: schema, err: err}
			}()
		}

		// Wait for all goroutines to be ready, then release them all at once
		ready.Wait()
		start.Done()

		// Collect results
		for i := 0; i < numReads; i++ {
			res := <-results
			require.NoError(t, res.err)
			require.NotNil(t, res.schema)
			require.Equal(t, "hash1", res.schema.GetV1().SchemaHash)
		}

		// CRITICAL: Should only have 1 actual datastore read due to singleflight
		// Allow up to 2 reads due to timing variations
		require.LessOrEqual(t, readCount.Load(), int32(2), "singleflight should deduplicate most concurrent revisioned reads")
	})

	t.Run("does not use singleflight for transactional reads", func(t *testing.T) {
		// Create a schema to return
		storedSchema := &core.StoredSchema{
			Version: 1,
			VersionOneof: &core.StoredSchema_V1{
				V1: &core.StoredSchema_V1StoredSchema{
					SchemaText: "definition user {}",
					SchemaHash: "hash1",
				},
			},
		}
		data, err := storedSchema.MarshalVT()
		require.NoError(t, err)

		// Create executor that counts reads (use atomic for thread safety)
		var readCount atomic.Int32
		executor := &fakeExecutor{
			readResult: map[int][]byte{0: data},
			onRead: func() {
				readCount.Add(1)
			},
		}

		// Create config and schema reader/writer
		config := SQLByteChunkerConfig[uint64]{
			TableName:         "schema",
			NameColumn:        "name",
			ChunkIndexColumn:  "chunk_index",
			ChunkDataColumn:   "chunk_data",
			MaxChunkSize:      1024 * 64,
			PlaceholderFormat: sq.Question,
			Executor:          executor,
			WriteMode:         WriteModeDeleteAndInsert,
		}

		schemaRW := NewSQLSchemaReaderWriter[uint64, testRevision](
			config,
			options.SchemaCacheOptions{
				MaximumCacheMemoryBytes: 0,
			},
		)

		// Make 5 concurrent reads with nil revision (transactional reads)
		const numReads = 5

		type result struct {
			schema *core.StoredSchema
			err    error
		}
		results := make(chan result, numReads)

		for i := 0; i < numReads; i++ {
			go func() {
				schema, err := schemaRW.ReadSchema(context.Background(), executor, nil)
				results <- result{schema: schema, err: err}
			}()
		}

		// Collect results
		for i := 0; i < numReads; i++ {
			res := <-results
			require.NoError(t, res.err)
			require.NotNil(t, res.schema)
		}

		// CRITICAL: Should have 5 actual datastore reads (no singleflight for nil revision)
		require.Equal(t, int32(numReads), readCount.Load(), "transactional reads should NOT use singleflight")
	})
}

// testRevision is a simple test revision type
type testRevision struct {
	id int64
}

func (r testRevision) Equal(other datastore.Revision) bool {
	otherTest, ok := other.(testRevision)
	if !ok {
		return false
	}
	return r.id == otherTest.id
}

func (r testRevision) GreaterThan(other datastore.Revision) bool {
	otherTest, ok := other.(testRevision)
	if !ok {
		return false
	}
	return r.id > otherTest.id
}

func (r testRevision) LessThan(other datastore.Revision) bool {
	otherTest, ok := other.(testRevision)
	if !ok {
		return false
	}
	return r.id < otherTest.id
}

func (r testRevision) String() string {
	return fmt.Sprintf("test-%d", r.id)
}

func (r testRevision) Key() string {
	return r.String()
}

func (r testRevision) ByteSortable() bool {
	return false
}
