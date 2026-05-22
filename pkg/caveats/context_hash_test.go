package caveats

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

// TestDistinctStructsHaveDistinctHashes is a regression test -
// under a previous version of the code the serializations were
// equivalent
func TestDistinctStructsHaveDistinctHashes(t *testing.T) {
	oneStruct, err := structpb.NewStruct(map[string]any{"x": []any{[]any{"a"}, "b"}})
	require.NoError(t, err)
	anotherStruct, err := structpb.NewStruct(map[string]any{"x": []any{"a", []any{}, "b"}})
	require.NoError(t, err)

	oneSerialization, err := StableContextStringForHashing(oneStruct)
	require.NoError(t, err)
	anotherSerialization, err := StableContextStringForHashing(anotherStruct)
	require.NoError(t, err)

	require.NotEqual(t, oneSerialization, anotherSerialization)
}

func TestSameStructsHaveSameHashes(t *testing.T) {
	// NOTE: these are distinct structs but with the same contents,
	// to ensure that their serializations aren't vacuously the same.
	oneStruct, err := structpb.NewStruct(map[string]any{"x": []any{[]any{"a"}, "b"}})
	require.NoError(t, err)
	anotherStruct, err := structpb.NewStruct(map[string]any{"x": []any{[]any{"a"}, "b"}})
	require.NoError(t, err)

	oneSerialization, err := StableContextStringForHashing(oneStruct)
	require.NoError(t, err)
	anotherSerialization, err := StableContextStringForHashing(anotherStruct)
	require.NoError(t, err)

	require.Equal(t, oneSerialization, anotherSerialization)
}
