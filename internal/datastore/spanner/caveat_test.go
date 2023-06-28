package spanner

import (
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/stretchr/testify/require"
)

func TestContextualizedCaveatFrom(t *testing.T) {
	res, err := ContextualizedCaveatFrom(spanner.NullString{StringVal: "", Valid: false}, spanner.NullJSON{Value: nil, Valid: true})
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = ContextualizedCaveatFrom(spanner.NullString{StringVal: "", Valid: true}, spanner.NullJSON{Value: nil, Valid: false})
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = ContextualizedCaveatFrom(spanner.NullString{StringVal: "", Valid: true}, spanner.NullJSON{Value: nil, Valid: true})
	require.NoError(t, err)
	require.Nil(t, res)

	res, err = ContextualizedCaveatFrom(spanner.NullString{StringVal: "test", Valid: true}, spanner.NullJSON{Value: nil, Valid: true})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "test", res.CaveatName)
	require.NotNil(t, res.Context)
	require.Empty(t, res.Context.Fields)

	res, err = ContextualizedCaveatFrom(
		spanner.NullString{StringVal: "test", Valid: true},
		spanner.NullJSON{Value: map[string]any{"key": "val"}, Valid: true})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "test", res.CaveatName)
	require.NotNil(t, res.Context)
	require.Len(t, res.Context.Fields, 1)
	require.Equal(t, "val", res.Context.Fields["key"].GetStringValue())
}
