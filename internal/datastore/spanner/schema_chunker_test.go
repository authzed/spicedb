package spanner

import (
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
)

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
