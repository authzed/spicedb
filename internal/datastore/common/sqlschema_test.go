package common

import (
	"context"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/pkg/datastore"
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
			ctx := t.Context()
			err = readerWriter.WriteStoredSchema(ctx, storedSchema)
			require.NoError(t, err)

			// Verify write was called
			require.NotEmpty(t, txn.capturedSQL)

			// Read schema back
			readSchema, err := readerWriter.ReadStoredSchema(ctx)
			require.NoError(t, err)
			require.NotNil(t, readSchema)

			// Verify
			require.Equal(t, storedSchema.Version, readSchema.Get().Version)
			require.NotNil(t, readSchema.Get().GetV1())
			require.Equal(t, tt.schemaText, readSchema.Get().GetV1().SchemaText)
			require.Equal(t, "test-hash", readSchema.Get().GetV1().SchemaHash)
			require.Len(t, readSchema.Get().GetV1().NamespaceDefinitions, len(tt.namespaces))
			require.Len(t, readSchema.Get().GetV1().CaveatDefinitions, len(tt.caveats))
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
	ctx := t.Context()
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
	ctx := t.Context()
	err = readerWriter.WriteStoredSchema(ctx, storedSchema)
	require.NoError(t, err)

	// Read schema back
	readSchema, err := readerWriter.ReadStoredSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, readSchema)
	require.Equal(t, "definition user {}", readSchema.Get().GetV1().SchemaText)
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
	schemaText := "definition user {}\ncaveat is_allowed(allowed bool) { allowed }"

	// Build stored schema directly
	storedSchema := &core.StoredSchema{
		Version: 1,
		VersionOneof: &core.StoredSchema_V1{
			V1: &core.StoredSchema_V1StoredSchema{
				SchemaText: schemaText,
				SchemaHash: "test-hash",
				NamespaceDefinitions: map[string]*core.NamespaceDefinition{
					"user": {Name: "user"},
				},
				CaveatDefinitions: map[string]*core.CaveatDefinition{
					"is_allowed": {Name: "is_allowed"},
				},
			},
		},
	}

	require.Equal(t, uint32(1), storedSchema.Version)

	v1 := storedSchema.GetV1()
	require.NotNil(t, v1)
	require.Equal(t, schemaText, v1.SchemaText)
	require.Len(t, v1.NamespaceDefinitions, 1)
	require.Len(t, v1.CaveatDefinitions, 1)

	// Marshal
	data, err := storedSchema.MarshalVT()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal
	unmarshaled := &core.StoredSchema{}
	err = unmarshaled.UnmarshalVT(data)
	require.NoError(t, err)
	require.NotNil(t, unmarshaled)
	require.True(t, storedSchema.EqualVT(unmarshaled))
}

func TestHelperFunctions(t *testing.T) {
	// Test NoTransactionID
	ctx := t.Context()
	result := NoTransactionID[uint64](ctx)
	require.Equal(t, uint64(0), result)

	// Test StaticTransactionID
	staticFunc := StaticTransactionID[uint64](12345)
	require.Equal(t, uint64(12345), staticFunc(ctx))

	// Test with any type
	anyResult := NoTransactionID[any](ctx)
	require.Nil(t, anyResult)
}

func TestNewSQLSingleStoreSchemaReaderWriterForTransactionIDs(t *testing.T) {
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

	chunker := MustNewSQLByteChunker(SQLByteChunkerConfig[uint64]{
		TableName:         "schema",
		NameColumn:        "name",
		ChunkIndexColumn:  "chunk_index",
		ChunkDataColumn:   "chunk_data",
		MaxChunkSize:      1024 * 64,
		PlaceholderFormat: sq.Question,
		Executor:          executor,
		WriteMode:         WriteModeInsertWithTombstones,
		CreatedAtColumn:   "created_txn",
		DeletedAtColumn:   "deleted_txn",
		AliveValue:        uint64(9223372036854775807),
	})

	readerWriter := NewSQLSingleStoreSchemaReaderWriterForTransactionIDs(chunker, StaticTransactionID[uint64](42))

	ctx := t.Context()
	err = readerWriter.WriteStoredSchema(ctx, storedSchema)
	require.NoError(t, err)

	readSchema, err := readerWriter.ReadStoredSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, readSchema)
	require.Equal(t, "definition user {}", readSchema.Get().GetV1().SchemaText)
}
