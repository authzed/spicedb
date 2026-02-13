package spanner

import (
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/require"
)

// TestChunkerInsertSQLFormat validates that the SQL chunker generates INSERT statements
// in the expected format that convertInsertToMutation assumes.
func TestChunkerInsertSQLFormat(t *testing.T) {
	// This test validates the format assumption in convertInsertToMutation.
	// The chunker should generate: INSERT INTO schema (name, chunk_index, chunk_data) VALUES (@p1, @p2, @p3)

	builder := sq.Insert(tableSchema).
		Columns(colSchemaName, colSchemaChunkIndex, colSchemaChunkData).
		Values("test_name", 0, []byte("test_data")).
		PlaceholderFormat(sq.AtP)

	sql, args, err := builder.ToSql()
	require.NoError(t, err)

	// Validate the SQL format
	expectedSQL := "INSERT INTO schema (name,chunk_index,chunk_data) VALUES (@p1,@p2,@p3)"
	require.Equal(t, expectedSQL, sql, "SQL format has changed - convertInsertToMutation needs updating")

	// Validate args order and count
	require.Len(t, args, 3, "Expected exactly 3 args")
	require.Equal(t, "test_name", args[0], "First arg should be name")
	require.Equal(t, 0, args[1], "Second arg should be chunk_index")
	require.Equal(t, []byte("test_data"), args[2], "Third arg should be chunk_data")
}

// TestChunkerDeleteSQLFormat validates that the SQL chunker generates DELETE statements
// in the expected format.
func TestChunkerDeleteSQLFormat(t *testing.T) {
	// This test validates the format for DELETE operations.
	// The chunker should generate: DELETE FROM schema WHERE name = @p1

	builder := sq.Delete(tableSchema).
		Where(sq.Eq{colSchemaName: "test_name"}).
		PlaceholderFormat(sq.AtP)

	sql, args, err := builder.ToSql()
	require.NoError(t, err)

	// Validate the SQL format
	expectedSQL := "DELETE FROM schema WHERE name = @p1"
	require.Equal(t, expectedSQL, sql, "DELETE SQL format has changed")

	// Validate args
	require.Len(t, args, 1, "Expected exactly 1 arg")
	require.Equal(t, "test_name", args[0], "First arg should be name")
}

// TestConvertInsertToMutation validates the convertInsertToMutation function.
func TestConvertInsertToMutation(t *testing.T) {
	txn := &spannerChunkedBytesTransaction{}

	tests := []struct {
		name        string
		sql         string
		args        []any
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid insert with 3 args",
			sql:         "INSERT INTO schema (name,chunk_index,chunk_data) VALUES (@p1,@p2,@p3)",
			args:        []any{"test_name", 0, []byte("data")},
			expectError: false,
		},
		{
			name:        "invalid - too few args",
			sql:         "INSERT INTO schema (name,chunk_index) VALUES (@p1,@p2)",
			args:        []any{"test_name", 0},
			expectError: true,
			errorMsg:    "expected 3 args",
		},
		{
			name:        "invalid - too many args",
			sql:         "INSERT INTO schema (name,chunk_index,chunk_data,extra) VALUES (@p1,@p2,@p3,@p4)",
			args:        []any{"test_name", 0, []byte("data"), "extra"},
			expectError: true,
			errorMsg:    "expected 3 args",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mutation, err := txn.convertInsertToMutation(tt.sql, tt.args)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errorMsg)
				require.Nil(t, mutation)
			} else {
				require.NoError(t, err)
				require.NotNil(t, mutation)
			}
		})
	}
}
